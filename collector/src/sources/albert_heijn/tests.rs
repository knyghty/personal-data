//! Drive the source against a fake AH API and the fake GitHub.

use alloc::sync::Arc;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, bail, ensure};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use reqwest::Response;
use serde_json::{Value, json};

use super::{Ah, AlbertHeijn, DETAIL_QUERY, ListItem, SEEN_KEY, TOKEN_KEY, Tokens};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing::{self, Recorded};
use crate::{storage, tri};

/// The shared state of the fake AH API.
type Fake = Arc<Mutex<Seen>>;

/// What the fake AH API holds and saw.
#[derive(Default)]
struct Seen {
    /// Codes exchanged.
    codes: Vec<String>,
    /// Receipt ids whose details were fetched.
    details: Vec<String>,
    /// Number of token replies so far; tokens are `a<n>` and `r<n>`.
    issued: u32,
    /// List offsets requested.
    offsets: Vec<u64>,
    /// Receipt ids and times, newest first.
    receipts: Vec<(String, String)>,
    /// Whether every refresh fails.
    refresh_fails: bool,
    /// Refresh tokens presented.
    refreshes: Vec<String>,
}

impl Seen {
    /// Answer a GraphQL query: a receipt page or the details of one receipt.
    fn graphql(&mut self, headers: &HeaderMap, body: &Value) -> Result<Json<Value>, StatusCode> {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
        };
        if header("x-client-name") != "appie-ios" || !header("user-agent").starts_with("Appie/") {
            return Err(StatusCode::BAD_REQUEST);
        }
        if header("authorization") != format!("Bearer a{}", self.issued) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let query = body
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let vars = body.get("variables").unwrap_or(&Value::Null);
        if query.contains("posReceiptsPage") {
            let offset = vars
                .get("offset")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            self.offsets.push(offset);
            let limit = vars
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let page: Vec<Value> = self
                .receipts
                .iter()
                .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                .take(usize::try_from(limit).unwrap_or(usize::MAX))
                .map(|receipt| json!({"id": receipt.0, "dateTime": receipt.1, "totalAmount": {"amount": 4.5_f64}, "storeAddress": {"city": "Amsterdam"}}))
                .collect();
            return Ok(Json(json!({"data": {"posReceiptsPage": {
                "pagination": {"totalElements": self.receipts.len()}, "posReceipts": page,
            }}})));
        }
        let id = vars
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.details.push(id.clone());
        Ok(Json(
            json!({"data": {"posReceiptDetails": {"id": id, "products": [], "total": {"amount": 4.5_f64}}}}),
        ))
    }

    /// Issue the next token pair.
    fn issue(&mut self) -> Json<Value> {
        self.issued = self.issued.saturating_add(1);
        Json(
            json!({"access_token": format!("a{}", self.issued), "refresh_token": format!("r{}", self.issued), "expires_in": 7199}),
        )
    }

    /// Add a receipt as the newest and forget the list and detail calls seen so far.
    fn new_receipt(&mut self, receipt: (String, String)) {
        self.offsets.clear();
        self.details.clear();
        self.receipts.insert(0, receipt);
    }

    /// Refresh the tokens when the presented refresh token is the current one.
    fn refresh(&mut self, body: &Value) -> Result<Json<Value>, (StatusCode, &'static str)> {
        let presented = body
            .get("refreshToken")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let current = format!("r{}", self.issued);
        self.refreshes.push(presented.clone());
        if self.refresh_fails || presented != current {
            return Err((
                StatusCode::UNAUTHORIZED,
                r#"{"message":"invalid refresh token"}"#,
            ));
        }
        Ok(self.issue())
    }

    /// Exchange a login code for the first token pair.
    fn token(&mut self, body: &Value) -> Result<Json<Value>, StatusCode> {
        if body.get("clientId") != Some(&json!("appie-ios")) {
            return Err(StatusCode::BAD_REQUEST);
        }
        self.codes.push(
            body.get("code")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        );
        Ok(self.issue())
    }
}

/// A failing refresh fails the run with a message asking for a new login, without the token.
#[tokio::test]
async fn a_failing_refresh_asks_for_a_new_login() -> Result<()> {
    let (app, recorded, fake) = setup(1).await;
    drop(tri!(login(&app, "c").await));
    let mut tokens = tri!(stored(&app));
    tokens.expires_at = 0;
    tri!(app.store.set(TOKEN_KEY, &tokens));
    lock(&fake).refresh_fails = true;
    let Err(error) = AlbertHeijn.hourly(&app).await else {
        bail!("the run succeeded with a failing refresh");
    };
    let message = format!("{error:#}");
    ensure!(
        message.contains("a new login is needed"),
        "message lacks the login hint: {message}"
    );
    ensure!(
        message.contains("401"),
        "message lacks the status: {message}"
    );
    ensure!(
        !message.contains("r1"),
        "message leaks the token: {message}"
    );
    ensure!(
        github(&recorded).trees.is_empty(),
        "something was committed"
    );
    ensure!(lock(&fake).offsets.is_empty(), "receipts were listed");
    Ok(())
}

/// A token about to expire is refreshed, and the new refresh token replaces the old one.
#[tokio::test]
async fn an_expiring_token_is_refreshed_and_the_new_refresh_token_kept() -> Result<()> {
    let (app, _, fake) = setup(1).await;
    drop(tri!(login(&app, "c").await));
    let mut tokens = tri!(stored(&app));
    tokens.expires_at = Utc::now().timestamp().saturating_add(300);
    tri!(app.store.set(TOKEN_KEY, &tokens));
    tri!(AlbertHeijn.hourly(&app).await);
    let refreshed = tri!(stored(&app));
    ensure!(
        (
            refreshed.access_token.as_str(),
            refreshed.refresh_token.as_str()
        ) == ("a2", "r2"),
        "tokens not refreshed"
    );
    // A fresh token is used as is.
    tri!(AlbertHeijn.hourly(&app).await);
    ensure!(lock(&fake).refreshes == ["r1"], "unexpected refreshes");
    ensure!(lock(&fake).details == ["t1"], "unexpected details");
    Ok(())
}

/// Paths of every file committed so far.
fn committed(recorded: &testing::Shared) -> Vec<String> {
    github(recorded).trees.iter().flatten().cloned().collect()
}

/// The route takes a bare code, a whole redirect URL, and rejects an empty code.
#[tokio::test]
async fn extracts_the_code() -> Result<()> {
    let (app, _, fake) = setup(0).await;
    ensure!(
        tri!(login(&app, "appie://login-exit?code=AbC").await).status() == 200,
        "the redirect URL was rejected"
    );
    ensure!(
        tri!(login(&app, "abc").await).status() == 200,
        "the bare code was rejected"
    );
    ensure!(
        tri!(login(&app, "appie://login-exit?code=").await).status() == 400,
        "the empty code was accepted"
    );
    ensure!(lock(&fake).codes == ["AbC", "abc"], "unexpected codes");
    Ok(())
}

/// The first runs fetch details oldest first, later runs only the new receipts.
#[tokio::test]
async fn first_runs_backfill_oldest_first_then_only_new_receipts() -> Result<()> {
    let (app, recorded, fake) = setup(5).await;
    drop(tri!(login(&app, "c").await));
    tri!(AlbertHeijn.hourly(&app).await);
    ensure!(lock(&fake).offsets == [0, 2, 4], "unexpected offsets");
    ensure!(
        lock(&fake).details == ["t1", "t2", "t3"],
        "unexpected details"
    );
    let files = committed(&recorded);
    ensure!(
        files.first().map(String::as_str)
            == Some("raw/albert_heijn/receipt/2026/10/01/20261001T101500.000Z-t1.json"),
        "unexpected first path"
    );
    let first_blob = github(&recorded).blobs.first().cloned().unwrap_or_default();
    let blob: Value = tri!(serde_json::from_str(&first_blob));
    ensure!(
        blob.pointer("/response/data/posReceiptDetails/id") == Some(&json!("t1")),
        "unexpected receipt id"
    );
    ensure!(
        blob.pointer("/request/list_item/dateTime") == Some(&json!("2026-10-01T10:15:00")),
        "unexpected list item"
    );
    ensure!(
        app.store.get::<Vec<String>>(SEEN_KEY)
            == Some(vec!["t1".to_owned(), "t2".to_owned(), "t3".to_owned()]),
        "unexpected seen ids"
    );

    // The second run lists down to the first seen receipt and finishes the backfill.
    lock(&fake).offsets.clear();
    tri!(AlbertHeijn.hourly(&app).await);
    ensure!(
        lock(&fake).offsets == [0, 2],
        "unexpected offsets on the second run"
    );
    ensure!(committed(&recorded).len() == 5, "backfill not finished");

    // Later runs read one page and fetch only new receipts.
    lock(&fake).new_receipt(receipt(6));
    tri!(AlbertHeijn.hourly(&app).await);
    tri!(AlbertHeijn.hourly(&app).await);
    ensure!(
        lock(&fake).offsets == [0, 0],
        "unexpected offsets on later runs"
    );
    ensure!(
        lock(&fake).details == ["t6"],
        "unexpected details on later runs"
    );
    ensure!(
        github(&recorded).trees.len() == 3,
        "unexpected number of commits"
    );
    Ok(())
}

/// Lock the fake GitHub's record, poisoned or not.
fn github(recorded: &testing::Shared) -> MutexGuard<'_, Recorded> {
    recorded.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A reply with errors and no data fails the run.
#[tokio::test]
async fn graphql_errors_without_data_fail_the_run() -> Result<()> {
    let api = Router::new().route(
        "/graphql",
        post(async || {
            Json(json!({"data": null, "errors": [{"message": "Cannot query field \"x\""}]}))
        }),
    );
    let url = testing::serve(api).await;
    let (github_url, _) = testing::fake_github().await;
    let app = testing::app_with(&github_url, |cfg| cfg.albert_heijn_url = url);
    let Err(error) = Ah { app: &app }
        .graphql("a", DETAIL_QUERY, json!({"id": "t1"}))
        .await
    else {
        bail!("the query succeeded without data");
    };
    ensure!(
        error.to_string().contains("Cannot query field"),
        "unexpected error: {error}"
    );
    Ok(())
}

/// Lock the fake's state, poisoned or not.
fn lock(fake: &Fake) -> MutexGuard<'_, Seen> {
    fake.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Log in through the route with whatever the browser showed.
async fn login(app: &Arc<App>, code: &str) -> Result<Response> {
    let base = testing::serve(AlbertHeijn.router().with_state(Arc::clone(app))).await;
    let response = tri!(
        reqwest::Client::new()
            .get(format!("{base}/albert_heijn/auth"))
            .query(&[("key", testing::ADMIN), ("code", code)])
            .send()
            .await
    );
    Ok(response)
}

/// Without tokens the run does nothing.
#[tokio::test]
async fn not_logged_in_is_skipped() -> Result<()> {
    let (app, recorded, fake) = setup(3).await;
    tri!(AlbertHeijn.hourly(&app).await);
    ensure!(
        github(&recorded).trees.is_empty(),
        "something was committed"
    );
    ensure!(lock(&fake).offsets.is_empty(), "receipts were listed");
    Ok(())
}

/// Receipt times with an offset are converted to UTC, times without are filed as if UTC.
#[test]
fn reads_receipt_times_with_and_without_offset() -> Result<()> {
    let time = |text: &str| {
        ListItem {
            date_time: text.to_owned(),
            id: String::new(),
        }
        .time()
        .map(|at| storage::path("albert_heijn", "receipt", at, None, "json"))
    };
    ensure!(
        time("2026-10-01T10:15:00").as_deref()
            == Some("raw/albert_heijn/receipt/2026/10/01/20261001T101500.000Z.json"),
        "time without offset"
    );
    ensure!(
        time("2026-10-01T10:15:00+02:00").as_deref()
            == Some("raw/albert_heijn/receipt/2026/10/01/20261001T081500.000Z.json"),
        "time with offset"
    );
    ensure!(time("yesterday").is_none(), "garbage parsed as a time");
    Ok(())
}

/// Receipt `number`, on day `number` of October 2026.
fn receipt(number: usize) -> (String, String) {
    (
        format!("t{number}"),
        format!("2026-10-{number:02}T10:15:00"),
    )
}

/// Start a fake AH API holding `receipts` receipts, and an `App` pointed at it and at the fake GitHub.
async fn setup(receipts: usize) -> (Arc<App>, testing::Shared, Fake) {
    let (github_url, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(Seen {
        receipts: (1..=receipts).rev().map(receipt).collect(),
        ..Seen::default()
    }));
    let api = Router::new()
        .route(
            "/mobile-auth/v1/auth/token",
            post(async |State(state): State<Fake>, Json(body): Json<Value>| {
                lock(&state).token(&body)
            }),
        )
        .route(
            "/mobile-auth/v1/auth/token/refresh",
            post(async |State(state): State<Fake>, Json(body): Json<Value>| {
                lock(&state).refresh(&body)
            }),
        )
        .route(
            "/graphql",
            post(
                async |State(state): State<Fake>, headers: HeaderMap, Json(body): Json<Value>| {
                    lock(&state).graphql(&headers, &body)
                },
            ),
        )
        .with_state(Arc::clone(&fake));
    let url = testing::serve(api).await;
    let app = testing::app_with(&github_url, |cfg| cfg.albert_heijn_url = url);
    (app, recorded, fake)
}

/// The stored tokens.
fn stored(app: &App) -> Result<Tokens> {
    let tokens = tri!(app.store.get(TOKEN_KEY).context("no tokens stored"));
    Ok(tokens)
}

/// The route exchanges a bare code or the whole redirect URL, and never shows a token.
#[tokio::test]
async fn the_route_exchanges_a_code_or_the_whole_redirect_url() -> Result<()> {
    let (app, _, fake) = setup(0).await;
    let response = tri!(login(&app, "appie://login-exit?code=abc%2D1&state=x").await);
    ensure!(response.status() == 200, "login failed");
    let text = tri!(response.text().await);
    ensure!(
        !text.contains("a1") && !text.contains("r1"),
        "the reply leaks a token: {text}"
    );
    let tokens = tri!(stored(&app));
    ensure!(
        (tokens.access_token.as_str(), tokens.refresh_token.as_str()) == ("a1", "r1"),
        "unexpected tokens"
    );
    ensure!(
        tokens.expires_at > Utc::now().timestamp().saturating_add(7000),
        "expiry not set"
    );
    ensure!(
        tri!(login(&app, " xyz ").await).status() == 200,
        "bare code rejected"
    );
    ensure!(lock(&fake).codes == ["abc-1", "xyz"], "unexpected codes");
    ensure!(
        app.store.redacted().get(TOKEN_KEY) == Some(&json!("<redacted>")),
        "tokens not redacted"
    );
    Ok(())
}

/// Without the admin key the route answers 401, without a code 400.
#[tokio::test]
async fn the_route_needs_the_key_and_a_code() -> Result<()> {
    let (app, _, fake) = setup(0).await;
    let base = testing::serve(AlbertHeijn.router().with_state(Arc::clone(&app))).await;
    let http = reqwest::Client::new();
    let url = format!("{base}/albert_heijn/auth");
    let wrong = tri!(
        http.get(&url)
            .query(&[("key", "x"), ("code", "c")])
            .send()
            .await
    );
    ensure!(wrong.status() == 401, "wrong key accepted");
    let empty = tri!(
        http.get(&url)
            .query(&[
                ("key", testing::ADMIN),
                ("code", "appie://login-exit?code="),
            ])
            .send()
            .await
    );
    ensure!(empty.status() == 400, "empty code accepted");
    ensure!(lock(&fake).codes.is_empty(), "a code was exchanged");
    Ok(())
}
