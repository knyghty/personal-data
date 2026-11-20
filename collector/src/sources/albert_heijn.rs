//! Pull Albert Heijn in-store receipts (kassabonnen) from AH's unofficial mobile API, as the Appie app does.
//!
//! Login happens once by hand in a browser: AH's WAF blocks scripted logins. `/albert_heijn/auth` exchanges the
//! code for tokens, and from then on the collector only refreshes them. Mirrors <https://github.com/gwillem/appie-go>.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::time::Duration;
use std::collections::HashMap;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Client name the iOS app sends.
const CLIENT_ID: &str = "appie-ios";
/// Client version the iOS app sends.
const CLIENT_VERSION: &str = "9.28";
/// Receipt details per run, oldest first, so the first backfill spreads over a few runs.
const DETAILS_PER_RUN: usize = if cfg!(test) { 3 } else { 100 };
/// The receipt detail query.
///
/// Introspection is off, so a field AH removed fails the whole query: drop it here.
/// appie-go queries id, memberId, products { id quantity name price amount }, discounts { name amount } and payments;
/// the rest comes from its schema dump of 2026-01-18.
const DETAIL_QUERY: &str = "query FetchReceipt($id: String!) {
  posReceiptDetails(id: $id) {
    id
    memberId
    storeInfo
    products {
      id quantity name
      weight { amount unit }
      price { amount }
      amount { amount }
      indicators { name discount percentage }
    }
    discounts { type name amount { amount } }
    discountTotal { amount }
    promotions { quantity name }
    total { amount }
    payments { method amount { amount } }
    vat { levels { percentage salesAmount { amount } amount { amount } } total { salesAmount { amount } amount { amount } } }
    transaction { store lane id operator dateTime }
    address { street houseNumber postalCode city countryCode }
  }
}";
/// The receipt list query, one page at a time.
const LIST_QUERY: &str = "query FetchPosReceipts($offset: Int!, $limit: Int!) {
  posReceiptsPage(pagination: {offset: $offset, limit: $limit}) {
    pagination { totalElements }
    posReceipts { id dateTime totalAmount { amount } storeAddress { street houseNumber postalCode city countryCode } }
  }
}";
/// Most list pages per run.
const MAX_PAGES: usize = 100;
/// Pause between detail requests.
const PACE_MS: u64 = if cfg!(test) { 0 } else { 500 };
/// Receipts per list page. appie-go asks for 100.
const PAGE_SIZE: usize = if cfg!(test) { 2 } else { 100 };
/// Serialises token use, so a refresh never races another one: each refresh invalidates the old refresh token.
static PULL_LOCK: Mutex<()> = Mutex::const_new(());
/// Refresh when the access token expires within this many seconds.
const REFRESH_MARGIN: i64 = 600;
/// Attempts per request when AH answers 5xx, waiting 5 s, then 10 s, between them.
const RETRIES: u64 = 3;
/// Pause after the first 5xx, doubled after the second.
const RETRY_PAUSE_MS: u64 = if cfg!(test) { 0 } else { 5000 };
/// Ids of every receipt committed so far.
const SEEN_KEY: &str = "albert_heijn/seen";
/// Directory name under `raw/`.
const SOURCE: &str = "albert_heijn";
/// Store key of the tokens.
const TOKEN_KEY: &str = "albert_heijn/token";
/// User agent the iOS app sends.
const USER_AGENT: &str = "Appie/9.28 (iPhone17,3; iPhone; CPU OS 26_1 like Mac OS X)";

/// The API as the iOS app uses it.
struct Ah<'app> {
    /// The app whose HTTP client, store and configuration to use.
    app: &'app App,
}

impl Ah<'_> {
    /// Exchange a login code for tokens and return the access token.
    async fn exchange(&self, code: &str) -> Result<String> {
        self.token(
            "/mobile-auth/v1/auth/token",
            &json!({"clientId": CLIENT_ID, "code": code}),
        )
        .await
    }

    /// Run a GraphQL query and return the reply verbatim.
    async fn graphql(&self, access: &str, query: &str, variables: Value) -> Result<String> {
        let body = json!({"query": query, "variables": variables});
        let (status, text) = tri!(self.post("/graphql", Some(access), &body).await);
        if !status.is_success() {
            bail!("/graphql failed: {status} {}", snippet(&text));
        }
        let reply: Value = tri!(serde_json::from_str(&text).context("parse GraphQL reply"));
        if reply
            .get("data")
            .and_then(Value::as_object)
            .is_none_or(|data| data.values().all(Value::is_null))
        {
            bail!(
                "GraphQL failed, check the queries in albert_heijn.rs: {}",
                reply.get("errors").unwrap_or(&Value::Null)
            );
        }
        if let Some(errors) = reply.get("errors") {
            warn!("albert_heijn: GraphQL errors: {errors}");
        }
        Ok(text)
    }

    /// POST JSON to the API the way the iOS app does, retrying 5xx. Returns the status and body.
    async fn post(
        &self,
        path: &str,
        bearer: Option<&str>,
        body: &Value,
    ) -> Result<(StatusCode, String)> {
        let mut attempt = 0_u64;
        loop {
            attempt = attempt.saturating_add(1);
            let mut request = self
                .app
                .http
                .post(format!("{}{path}", self.app.cfg.albert_heijn_url))
                .header("User-Agent", USER_AGENT)
                .header("x-client-name", CLIENT_ID)
                .header("x-client-version", CLIENT_VERSION)
                .header("x-application", "AHWEBSHOP")
                .header("Accept", "application/json")
                .json(body);
            if let Some(token) = bearer {
                request = request.bearer_auth(token);
            }
            let response = tri!(
                request
                    .send()
                    .await
                    .with_context(|| format!("request {path}"))
            );
            let status = response.status();
            let text = tri!(response.text().await);
            if attempt >= RETRIES || !status.is_server_error() {
                return Ok((status, text));
            }
            warn!("albert_heijn: {path} {status}, retrying");
            sleep(Duration::from_millis(
                RETRY_PAUSE_MS.saturating_mul(attempt),
            ))
            .await;
        }
    }

    /// Refresh the access token and return the new one.
    async fn refresh(&self, refresh_token: &str) -> Result<String> {
        info!("albert_heijn: refreshing access token");
        self.token(
            "/mobile-auth/v1/auth/token/refresh",
            &json!({"clientId": CLIENT_ID, "refreshToken": refresh_token}),
        )
        .await
        .context("token refresh failed, a new login is needed: log in by hand and pass the code to /albert_heijn/auth (README, Albert Heijn)")
    }

    /// Exchange a code or refresh token and store the new tokens before anything else, since the old refresh token
    /// stops working. Returns the access token. Never puts a token in an error.
    async fn token(&self, path: &str, body: &Value) -> Result<String> {
        let (status, text) = tri!(self.post(path, None, body).await);
        if !status.is_success() {
            bail!("{path} failed: {status} {}", snippet(&text));
        }
        let reply: TokenReply = tri!(serde_json::from_str(&text).with_context(|| format!(
            "{path}: reply without access_token, refresh_token or expires_in"
        )));
        tri!(self.app.store.set(
            TOKEN_KEY,
            &Tokens {
                access_token: reply.access_token.clone(),
                expires_at: Utc::now().timestamp().saturating_add(reply.expires_in),
                refresh_token: reply.refresh_token,
            },
        ));
        Ok(reply.access_token)
    }

    /// List receipts newest first down to the first page holding a seen id, and return the unseen ones.
    async fn unseen(&self, access: &str, seen: &[String]) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        for page in 0..MAX_PAGES {
            let offset = page.saturating_mul(PAGE_SIZE);
            let text = tri!(
                self.graphql(
                    access,
                    LIST_QUERY,
                    json!({"offset": offset, "limit": PAGE_SIZE}),
                )
                .await
            );
            let reply: Value = tri!(serde_json::from_str(&text));
            let list = reply
                .pointer("/data/posReceiptsPage")
                .unwrap_or(&Value::Null);
            let items = list
                .get("posReceipts")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let metas: Vec<ListItem> =
                tri!(serde_json::from_value(json!(items)).context("receipt list"));
            // Stopping at a seen id is only right when the list is newest first.
            if !metas.is_sorted_by(|newer, older| newer.date_time >= older.date_time) {
                bail!("the receipt list is not newest first, so paging would miss receipts");
            }
            let reached = metas.iter().any(|meta| seen.contains(&meta.id));
            out.extend(
                items
                    .into_iter()
                    .zip(&metas)
                    .filter(|pair| !seen.contains(&pair.1.id))
                    .map(|(item, _)| item),
            );
            let total = list
                .pointer("/pagination/totalElements")
                .and_then(Value::as_u64)
                .map_or(usize::MAX, |count| {
                    usize::try_from(count).unwrap_or(usize::MAX)
                });
            if reached || metas.len() < PAGE_SIZE || offset.saturating_add(PAGE_SIZE) >= total {
                break;
            }
        }
        Ok(out)
    }
}

/// The Albert Heijn source.
pub(super) struct AlbertHeijn;

#[async_trait]
impl Source for AlbertHeijn {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let _guard = PULL_LOCK.lock().await;
        let Some(tokens) = app.store.get::<Tokens>(TOKEN_KEY) else {
            info!("albert_heijn: not logged in, skipping");
            return Ok(());
        };
        let ah = Ah { app };
        let access = if tokens.expires_at.saturating_sub(Utc::now().timestamp()) < REFRESH_MARGIN {
            tri!(ah.refresh(&tokens.refresh_token).await)
        } else {
            tokens.access_token
        };
        let mut seen: Vec<String> = app.store.get(SEEN_KEY).unwrap_or_default();
        let mut fresh = tri!(ah.unseen(&access, &seen).await);
        // Oldest first: the committed receipts then stay one unbroken stretch, so listing can stop at the first seen id.
        fresh.reverse();
        fresh.truncate(DETAILS_PER_RUN);
        let now = Utc::now();
        let mut files = Vec::new();
        for (index, item) in fresh.iter().enumerate() {
            if index > 0 {
                sleep(Duration::from_millis(PACE_MS)).await;
            }
            let meta: ListItem =
                tri!(serde_json::from_value(item.clone()).context("receipt list item"));
            let text = tri!(
                ah.graphql(&access, DETAIL_QUERY, json!({"id": meta.id}))
                    .await
            );
            files.push(File {
                path: storage::path(
                    SOURCE,
                    "receipt",
                    meta.time().unwrap_or(now),
                    Some(&meta.id),
                    "json",
                ),
                content: tri!(storage::envelope(
                    now,
                    json!({"query": "FetchReceipt", "id": meta.id, "list_item": item}),
                    &text,
                )),
            });
            seen.push(meta.id);
        }
        let message = format!("albert_heijn: receipt ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        if !files.is_empty() {
            tri!(app.store.set(SEEN_KEY, &seen));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new().route(
            "/albert_heijn/auth",
            get(
                async |State(app): State<Arc<App>>,
                       Query(query): Query<HashMap<String, String>>|
                       -> Response {
                    if query.get("key") != Some(&app.cfg.admin_secret) {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    // The code from what the browser showed: the bare code, or the whole `appie://login-exit?code=...` URL.
                    let shown = query.get("code").map_or("", |value| value.trim());
                    let raw = shown.split_once("code=").map_or(shown, |(_, rest)| {
                        rest.split(['&', '#']).next().unwrap_or("")
                    });
                    let code = urlencoding::decode(raw)
                        .map_or_default(|decoded| decoded.trim().to_owned());
                    if code.is_empty() {
                        return (StatusCode::BAD_REQUEST, "missing code\n").into_response();
                    }
                    let _guard = PULL_LOCK.lock().await;
                    let exchanged = Ah { app: &app }.exchange(&code).await;
                    match exchanged {
                        Ok(_) => "Albert Heijn connected. The next run pulls all receipts.\n"
                            .into_response(),
                        Err(error) => (
                            StatusCode::BAD_GATEWAY,
                            format!(
                                "code exchange failed, log in again for a new code: {error:#}\n"
                            ),
                        )
                            .into_response(),
                    }
                },
            ),
        )
    }
}

/// One receipt as the list query returns it.
#[derive(Deserialize)]
struct ListItem {
    /// Receipt time as AH writes it, with or without an offset.
    #[serde(rename = "dateTime")]
    date_time: String,
    /// Receipt id.
    id: String,
}

impl ListItem {
    /// The receipt time for the file name.
    ///
    /// appie-go's samples carry no offset; such a time is Amsterdam time but is filed as if UTC, so the date folder
    /// matches the receipt.
    fn time(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.date_time)
            .map(|parsed| parsed.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                NaiveDateTime::parse_from_str(&self.date_time, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(|naive| naive.and_utc())
            })
    }
}

/// A token reply from the mobile auth API.
#[derive(Deserialize)]
struct TokenReply {
    /// Bearer token for the API.
    access_token: String,
    /// Seconds until `access_token` expires.
    expires_in: i64,
    /// Token for the next refresh.
    refresh_token: String,
}

/// The stored tokens.
#[derive(Serialize, Deserialize)]
struct Tokens {
    /// Bearer token for the API.
    access_token: String,
    /// Unix time at which `access_token` expires.
    expires_at: i64,
    /// Token for the next refresh.
    refresh_token: String,
}

/// The first 300 characters of a reply, for errors.
fn snippet(text: &str) -> String {
    text.chars().take(300).collect()
}
