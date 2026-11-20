//! Run the bunq source against a fake API, and once against the sandbox.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use rsa::RsaPublicKey;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePublicKey as _;
use rsa::signature::Verifier as _;
use serde_json::{Value, json};
use sha2::Sha256;
use tracing::info;

use super::{BUSINESS, Installation, PERSONAL, Session};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// The payment list of the fake's one monetary account.
const PAYMENTS: &str = "/v1/user/42/monetary-account/7/payment";

/// The fake behind the HTTP handlers.
type Fake = Arc<Mutex<Seen>>;

/// What the fake bunq saw.
#[derive(Default)]
struct Seen {
    /// The public key of the installation.
    client_public_key: Option<String>,
    /// The notification filters set last.
    filters: Vec<Value>,
    /// Sessions started.
    sessions: usize,
    /// Session tokens that the fake rejects with 401.
    stale: Vec<String>,
    /// Every list URL asked, with its query string.
    urls: Vec<String>,
}

impl Seen {
    /// Answer a monetary account list: three pages of payments, every other list empty.
    fn account_list(
        &mut self,
        kind: &str,
        headers: &HeaderMap,
        raw: &str,
        query: &HashMap<String, String>,
    ) -> (StatusCode, Json<Value>) {
        let token = header(headers, "x-bunq-client-authentication");
        if self.stale.contains(&token) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"Error": [{"error_description": "Insufficient authentication."}]})),
            );
        }
        self.urls
            .push(format!("/v1/user/42/monetary-account/7/{kind}?{raw}"));
        if kind != "payment" {
            return (StatusCode::OK, Json(empty_page()));
        }
        let older_id = query.get("older_id").map(String::as_str);
        let newer_id = query.get("newer_id").map(String::as_str);
        let (items, older, newer) = match (older_id, newer_id) {
            (None, None) => (
                payments(&[10, 9, 8]),
                Some(format!("{PAYMENTS}?count=200&older_id=8")),
                None,
            ),
            (Some("8"), None) => (payments(&[7, 6]), None, None),
            (None, Some("10")) => (
                payments(&[12, 11]),
                None,
                Some(format!("{PAYMENTS}?count=200&newer_id=12")),
            ),
            (None, Some(_)) => (vec![], None, None),
            (Some(_), _) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"Error": [{"error_description": "unexpected page"}]})),
                );
            }
        };
        let page = json!({"Response": items, "Pagination": {"older_url": older, "newer_url": newer, "future_url": null}});
        (StatusCode::OK, Json(page))
    }

    /// Start a session for a signed request with the installation token.
    fn new_session(&mut self, headers: &HeaderMap, body: &str) -> Json<Value> {
        assert_eq!(
            header(headers, "x-bunq-client-authentication"),
            "inst-token",
            "installation token"
        );
        assert!(verify(self, headers, body.as_bytes()), "valid signature");
        self.sessions = self.sessions.saturating_add(1);
        let token = format!("sess-{}", self.sessions);
        Json(
            json!({"Response": [{"Id": {"id": 3_i64}}, {"Token": {"token": token}}, {"UserPerson": {"id": 42_i64, "session_timeout": 604_800_i64}}]}),
        )
    }

    /// Replace the notification filters from a signed request.
    fn set_filters(&mut self, headers: &HeaderMap, body: &str) -> Json<Value> {
        assert!(verify(self, headers, body.as_bytes()), "valid signature");
        let request: Value = serde_json::from_str(body).unwrap_or_default();
        self.filters = request
            .get("notification_filters")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Json(json!({"Response": [{"Id": {"id": 5_i64}}]}))
    }
}

/// A page without items or links.
fn empty_page() -> Value {
    json!({"Response": [], "Pagination": {"older_url": null, "newer_url": null, "future_url": null}})
}

/// Start the fake bunq API on a random port.
async fn fake_bunq() -> (String, Fake) {
    let fake: Fake = Arc::default();
    let router = Router::new()
        .route(
            "/v1/installation",
            post(async |State(seen): State<Fake>, Json(body): Json<Value>| {
                lock(&seen).client_public_key = body
                    .get("client_public_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                Json(json!({"Response": [{"Id": {"id": 1_i64}}, {"Token": {"token": "inst-token"}}, {"ServerPublicKey": {"server_public_key": "server-pem"}}]}))
            }),
        )
        .route(
            "/v1/device-server",
            post(async |State(seen): State<Fake>, headers: HeaderMap, body: String| {
                assert_eq!(
                    header(&headers, "x-bunq-client-authentication"),
                    "inst-token",
                    "installation token"
                );
                let request: Value = serde_json::from_str(&body).unwrap_or_default();
                assert_eq!(
                    request.get("permitted_ips"),
                    Some(&json!(["*"])),
                    "permitted ips"
                );
                assert!(
                    verify(&lock(&seen), &headers, body.as_bytes()),
                    "valid signature"
                );
                Json(json!({"Response": [{"Id": {"id": 2_i64}}]}))
            }),
        )
        .route(
            "/v1/session-server",
            post(async |State(seen): State<Fake>, headers: HeaderMap, body: String| {
                lock(&seen).new_session(&headers, &body)
            }),
        )
        .route(
            "/v1/user/42/monetary-account/7/{kind}",
            get(
                async |State(seen): State<Fake>,
                       headers: HeaderMap,
                       Path(kind): Path<String>,
                       RawQuery(raw): RawQuery,
                       Query(query): Query<HashMap<String, String>>| {
                    lock(&seen).account_list(&kind, &headers, &raw.unwrap_or_default(), &query)
                },
            ),
        )
        .route(
            "/v1/user/42/invoice",
            get(async |State(seen): State<Fake>, RawQuery(raw): RawQuery| {
                lock(&seen)
                    .urls
                    .push(format!("/v1/user/42/invoice?{}", raw.unwrap_or_default()));
                Json(empty_page())
            }),
        )
        .route(
            "/v1/user/42/notification-filter-url",
            get(async |State(seen): State<Fake>| {
                let items: Vec<Value> = lock(&seen)
                    .filters
                    .iter()
                    .map(|filter| json!({"NotificationFilterUrl": filter}))
                    .collect();
                Json(json!({"Response": items}))
            })
            .post(async |State(seen): State<Fake>, headers: HeaderMap, body: String| {
                lock(&seen).set_filters(&headers, &body)
            }),
        )
        .route(
            "/v1/user/42/monetary-account",
            get(async || {
                Json(json!({"Response": [{"MonetaryAccountBank": {"id": 7_i64, "balance": {"currency": "EUR", "value": "1.00"}}}]}))
            }),
        )
        .route(
            "/v1/user/42/card",
            get(async || Json(json!({"Response": []}))),
        )
        .with_state(Arc::clone(&fake));
    let base = testing::serve(router).await;
    (base, fake)
}

/// A header as text, empty when missing.
fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// Lock a test mutex, ignoring poisoning.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Every committed path, in commit order.
fn paths(recorded: &testing::Shared) -> Vec<String> {
    lock(recorded).trees.iter().flatten().cloned().collect()
}

/// Payment items with these ids.
fn payments(ids: &[i64]) -> Vec<Value> {
    ids.iter()
        .map(|id| json!({"Payment": {"id": id, "amount": {"currency": "EUR", "value": "-1.00"}}}))
        .collect()
}

/// An app with the fake GitHub and the fake bunq, with the personal API key set.
async fn setup() -> (Arc<App>, testing::Shared, Fake) {
    let (github, recorded) = testing::fake_github().await;
    let (bunq, fake) = fake_bunq().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.bunq_api_key = Some("key-1".into());
        cfg.bunq_api_url = bunq;
    });
    (app, recorded, fake)
}

/// Whether the request body is signed by the installation's key.
fn verify(seen: &Seen, headers: &HeaderMap, body: &[u8]) -> bool {
    let Some(pem) = seen.client_public_key.as_deref() else {
        return false;
    };
    let Ok(public) = RsaPublicKey::from_public_key_pem(pem) else {
        return false;
    };
    let Ok(encoded) = BASE64_STANDARD.decode(header(headers, "x-bunq-client-signature")) else {
        return false;
    };
    let Ok(signature) = Signature::try_from(encoded.as_slice()) else {
        return false;
    };
    let checked = VerifyingKey::<Sha256>::new(public).verify(body, &signature);
    checked.is_ok()
}

/// The first run registers the installation and device, starts a session and backfills every list.
#[tokio::test]
async fn first_run_installs_registers_and_backfills() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(PERSONAL.hourly(&app).await);
    let seen = lock(&fake);
    ensure!(seen.client_public_key.is_some(), "installed");
    ensure!(seen.sessions == 1, "one session");
    ensure!(
        seen.urls
            == [
                "/v1/user/42/invoice?count=200".to_owned(),
                format!("{PAYMENTS}?count=200"),
                format!("{PAYMENTS}?count=200&older_id=8"),
                "/v1/user/42/monetary-account/7/mastercard-action?count=200".to_owned(),
                "/v1/user/42/monetary-account/7/request-inquiry?count=200".to_owned(),
                "/v1/user/42/monetary-account/7/request-response?count=200".to_owned(),
            ],
        "urls: {}",
        seen.urls.join(", ")
    );
    drop(seen);
    let committed = paths(&recorded);
    ensure!(committed.len() == 2, "two pages committed");
    ensure!(
        committed
            .iter()
            .all(|path| path.starts_with("raw/bunq/payment/") && path.ends_with("-7.json")),
        "payment files"
    );
    ensure!(
        app.store.get::<i64>("bunq/cursor/payment/7") == Some(10),
        "cursor at the newest id"
    );
    ensure!(
        app.store.get::<String>("bunq/older/payment/7").is_none(),
        "backfill finished"
    );
    ensure!(
        app.store.get::<i64>("bunq/cursor/invoice").is_none(),
        "no invoice cursor"
    );
    ensure!(
        app.store
            .get::<Installation>("bunq/installation_secret")
            .is_some(),
        "installation stored"
    );
    ensure!(
        app.store.redacted().get("bunq/installation_secret") == Some(&json!("<redacted>")),
        "installation redacted"
    );
    Ok(())
}

/// The second run reuses the session and asks only for items newer than the cursor.
#[tokio::test]
async fn later_runs_pull_newer_events_and_reuse_the_session() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(PERSONAL.hourly(&app).await);
    lock(&fake).urls.clear();
    tri!(PERSONAL.hourly(&app).await);
    let seen = lock(&fake);
    ensure!(seen.sessions == 1, "session reused");
    let payment_urls: Vec<&String> = seen
        .urls
        .iter()
        .filter(|url| url.contains("/payment?"))
        .collect();
    ensure!(
        payment_urls
            == [
                &format!("{PAYMENTS}?count=200&newer_id=10"),
                &format!("{PAYMENTS}?count=200&newer_id=12")
            ],
        "payment urls: {}",
        seen.urls.join(", ")
    );
    drop(seen);
    ensure!(paths(&recorded).len() == 3, "one more page committed");
    ensure!(
        app.store.get::<i64>("bunq/cursor/payment/7") == Some(12),
        "cursor moved"
    );
    Ok(())
}

/// A 401 starts a new session once and stores it.
#[tokio::test]
async fn rejected_session_is_renewed_once() -> Result<()> {
    let (app, _, fake) = setup().await;
    tri!(PERSONAL.hourly(&app).await);
    lock(&fake).stale.push("sess-1".into());
    tri!(PERSONAL.hourly(&app).await);
    ensure!(lock(&fake).sessions == 2, "two sessions");
    let session: Option<Session> = app.store.get("bunq/session_token");
    ensure!(
        session.map(|stored| stored.token) == Some("sess-2".to_owned()),
        "the new session is stored"
    );
    Ok(())
}

/// A changed API key registers a new installation.
#[tokio::test]
async fn rotated_api_key_makes_a_new_installation() -> Result<()> {
    let (app, _, fake) = setup().await;
    tri!(PERSONAL.hourly(&app).await);
    let first: Installation = tri!(
        app.store
            .get("bunq/installation_secret")
            .context("first installation")
    );
    let (github, _) = testing::fake_github().await;
    let base = app.cfg.bunq_api_url.clone();
    let rotated = testing::app_with(&github, |cfg| {
        cfg.bunq_api_key = Some("key-2".into());
        cfg.bunq_api_url = base;
    });
    tri!(rotated.store.set("bunq/installation_secret", &first));
    tri!(PERSONAL.hourly(&rotated).await);
    let second: Installation = tri!(
        rotated
            .store
            .get("bunq/installation_secret")
            .context("second installation")
    );
    ensure!(
        first.api_key_sha256 != second.api_key_sha256,
        "a new installation"
    );
    ensure!(lock(&fake).sessions == 2, "a new session");
    Ok(())
}

/// The daily job registers the callback once and commits the account snapshot.
#[tokio::test]
async fn daily_registers_the_callback_once_and_snapshots() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(PERSONAL.daily(&app).await);
    tri!(PERSONAL.daily(&app).await);
    let token: String = tri!(app.store.get("bunq/notify_token").context("notify token"));
    let filters = lock(&fake).filters.clone();
    ensure!(filters.len() == 1, "one filter");
    ensure!(
        filters.first().and_then(|filter| filter.get("category")) == Some(&json!("MUTATION")),
        "category"
    );
    ensure!(
        filters
            .first()
            .and_then(|filter| filter.get("notification_target"))
            == Some(&json!(format!("http://localhost/bunq/notify/{token}"))),
        "target"
    );
    let committed = paths(&recorded);
    ensure!(committed.len() == 2, "two snapshots");
    ensure!(
        committed
            .iter()
            .all(|path| path.starts_with("raw/bunq/monetary_account/")),
        "account snapshots only"
    );
    ensure!(
        app.store.redacted().get("bunq/notify_token") == Some(&json!("<redacted>")),
        "token redacted"
    );
    Ok(())
}

/// The callback refuses a wrong token, and with the right one stores the body and pulls what is new.
#[tokio::test]
async fn notification_needs_the_token_and_pulls_new_events() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(PERSONAL.hourly(&app).await);
    tri!(app.store.set("bunq/notify_token", &"abc"));
    let url = testing::serve(PERSONAL.router().with_state(Arc::clone(&app))).await;
    let http = reqwest::Client::new();
    let wrong = tri!(
        http.post(format!("{url}/bunq/notify/nope"))
            .body("{}")
            .send()
            .await
    );
    ensure!(wrong.status() == StatusCode::UNAUTHORIZED, "wrong token");
    let right = tri!(
        http.post(format!("{url}/bunq/notify/abc"))
            .body(r#"{"NotificationUrl": {"category": "MUTATION"}}"#)
            .send()
            .await
    );
    ensure!(right.status() == StatusCode::OK, "right token");
    ensure!(app.tasks.close(), "the tracker was open");
    app.tasks.wait().await;
    ensure!(
        paths(&recorded)
            .iter()
            .any(|path| path.starts_with("raw/bunq/notification/")),
        "notification stored"
    );
    ensure!(
        app.store.get::<i64>("bunq/cursor/payment/7") == Some(12),
        "new events pulled"
    );
    ensure!(
        lock(&fake)
            .urls
            .iter()
            .any(|asked| asked.contains("newer_id=10")),
        "asked for newer items"
    );
    Ok(())
}

/// The business user has its own files, store keys and callback route.
#[tokio::test]
async fn business_user_has_its_own_files_state_and_route() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let (bunq, fake) = fake_bunq().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.bunq_business_api_key = Some("key-b".into());
        cfg.bunq_api_url = bunq;
    });
    tri!(PERSONAL.hourly(&app).await);
    ensure!(lock(&fake).sessions == 0, "personal is not configured");
    tri!(BUSINESS.hourly(&app).await);
    tri!(BUSINESS.daily(&app).await);
    ensure!(
        paths(&recorded)
            .iter()
            .all(|path| path.starts_with("raw/bunq_business/")),
        "business files"
    );
    ensure!(
        app.store.get::<i64>("bunq_business/cursor/payment/7") == Some(10),
        "business cursor"
    );
    ensure!(
        app.store.get::<i64>("bunq/cursor/payment/7").is_none(),
        "no personal cursor"
    );
    let token: String = tri!(
        app.store
            .get("bunq_business/notify_token")
            .context("notify token")
    );
    let target = lock(&fake)
        .filters
        .first()
        .and_then(|filter| filter.get("notification_target"))
        .cloned();
    ensure!(
        target
            == Some(json!(format!(
                "http://localhost/bunq_business/notify/{token}"
            ))),
        "business callback"
    );
    let url = testing::serve(BUSINESS.router().with_state(Arc::clone(&app))).await;
    let answer = tri!(
        reqwest::Client::new()
            .post(format!("{url}/bunq_business/notify/{token}"))
            .body("{}")
            .send()
            .await
    );
    ensure!(answer.status() == StatusCode::OK, "business route");
    Ok(())
}

/// `find` reads the object under a key of the `Response` array.
#[test]
fn find_reads_the_response_array() -> Result<()> {
    let answer = json!({"Response": [{"Id": {"id": 1_i64}}, {"Token": {"token": "t"}}]});
    ensure!(
        super::find(&answer, "Token").get("token") == Some(&json!("t")),
        "token found"
    );
    ensure!(super::find(&answer, "Missing").is_null(), "missing is null");
    Ok(())
}

/// End to end against the bunq sandbox.
///
/// Run with `BUNQ_SANDBOX_KEY=sandbox_... cargo test live_sandbox -- --ignored --nocapture`.
/// Set `BUNQ_LIVE_OUT` to a directory to keep the files that would have been committed.
#[tokio::test]
#[ignore = "needs BUNQ_SANDBOX_KEY and the network"]
async fn live_sandbox() -> Result<()> {
    let key = tri!(env::var("BUNQ_SANDBOX_KEY").context("BUNQ_SANDBOX_KEY"));
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.bunq_api_key = Some(key);
        cfg.bunq_api_url = "https://public-api.sandbox.bunq.com".into();
        // bunq checks that the callback host resolves. The real app answers 401 to the unknown token.
        cfg.public_url = "https://glimmering-voice-610.fly.dev".into();
    });
    tri!(PERSONAL.hourly(&app).await);
    tri!(PERSONAL.hourly(&app).await);
    tri!(PERSONAL.daily(&app).await);
    info!("state: {}", app.store.redacted());
    let seen = lock(&recorded);
    info!("commits: {}", seen.commit_messages.join(", "));
    let committed: Vec<String> = seen.trees.iter().flatten().cloned().collect();
    info!("files: {}", committed.join(", "));
    if let Ok(dir) = env::var("BUNQ_LIVE_OUT") {
        for (path, blob) in committed.iter().zip(&seen.blobs) {
            let target = PathBuf::from(&dir).join(path);
            if let Some(parent) = target.parent() {
                tri!(fs::create_dir_all(parent));
            }
            tri!(fs::write(target, blob));
        }
    }
    Ok(())
}
