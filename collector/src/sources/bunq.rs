//! Pull every bunq event (payments, card transactions, requests, interest) plus daily account and card snapshots, with a callback for fast updates.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::time::Duration;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse as _;
use axum::routing::post;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use chrono::{DateTime, TimeDelta, Utc};
use rand::Rng as _;
use reqwest::Method;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _, EncodePublicKey as _, LineEnding};
use rsa::signature::{SignatureEncoding as _, Signer as _};
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use super::Source;
use crate::app::App;
use crate::storage::{self, File, count};
use crate::tri;

/// Lists pulled per monetary account: file type and URL segment. Together they hold every money movement.
///
/// The user event feed is not used: it hides card settlements and savings transfers, and its
/// auto-save entries make it a hundred times larger.
const ACCOUNT_LISTS: &[(&str, &str)] = &[
    ("payment", "payment"),
    ("mastercard_action", "mastercard-action"),
    ("request_inquiry", "request-inquiry"),
    ("request_response", "request-response"),
];
/// The business user, a separate API key, files and state.
pub(super) const BUSINESS: Bunq = Bunq {
    name: "bunq_business",
};
/// Callback category: every change of a monetary account balance.
const CATEGORY: &str = "MUTATION";
/// bunq rejects any other modulus length.
const KEY_BITS: usize = 2048;
/// bunq allows 3 GET requests per 3 seconds per endpoint.
const PACE_MS: u64 = if cfg!(test) { 0 } else { 1100 };
/// Items per page, the API's maximum.
const PAGE: usize = 200;
/// Most pages per run over all lists. A backfill continues on the next run.
const PAGE_CAP: usize = 100;
/// Pages per commit, so a long backfill keeps its progress.
const PAGE_CHUNK: usize = 10;
/// The personal user.
pub(super) const PERSONAL: Bunq = Bunq { name: "bunq" };
/// Serialises pulls, so a callback and the hourly job never read a cursor the other is about to move.
static PULL_LOCK: Mutex<()> = Mutex::const_new(());
/// Lists pulled per user.
const USER_LISTS: &[(&str, &str)] = &[("invoice", "invoice")];

/// One bunq user. A business account is a separate user with its own API key, files and state.
pub(super) struct Bunq {
    /// Directory under `raw/`, prefix of the store keys and of the callback route.
    name: &'static str,
}

/// A signed-in bunq user for one job.
struct Client<'app> {
    /// The API key of the user.
    api_key: String,
    /// The shared clients and configuration.
    app: &'app App,
    /// Origin of the API.
    base: String,
    /// The user's name, as `Bunq::name`.
    name: &'static str,
}

/// One list to pull: file type, base URL and the store key that holds its cursors.
struct Feed {
    /// The monetary account the list belongs to; `None` for a user list.
    account: Option<i64>,
    /// The store key suffix of the cursors.
    key: String,
    /// The file type under `raw/`.
    kind: &'static str,
    /// The URL path of the list.
    path: String,
}

/// The RSA key pair registered with bunq and the tokens that came back. Stored under a `secret` key, so it is redacted.
#[derive(Serialize, Deserialize)]
struct Installation {
    /// Which API key the device was registered with, so a rotated key triggers a new installation.
    api_key_sha256: String,
    /// The private key, PKCS#8 PEM.
    private_key: String,
    /// bunq's public key, PEM.
    server_public_key: String,
    /// The installation token.
    token: String,
}

/// Files and cursor updates gathered by a pull, committed in chunks. Cursors move only after their files are committed.
#[derive(Default)]
struct Pull {
    /// Pages not yet committed.
    files: Vec<File>,
    /// Newest item id seen per feed key.
    newest: Vec<(String, i64)>,
    /// Where the backfill of a feed continues, or `None` when it is finished.
    older: Vec<(String, Option<String>)>,
}

/// A session token and the user it belongs to.
#[derive(Serialize, Deserialize, Clone)]
struct Session {
    /// When bunq stops accepting the token.
    expires_at: DateTime<Utc>,
    /// The session token.
    token: String,
    /// The bunq user id.
    user_id: i64,
}

impl Bunq {
    /// A client for this user when its API key is configured.
    fn client<'app>(&self, app: &'app App) -> Option<Client<'app>> {
        let configured = match self.name {
            "bunq" => app.cfg.bunq_api_key.clone(),
            _ => app.cfg.bunq_business_api_key.clone(),
        };
        configured.map(|api_key| Client {
            api_key,
            app,
            base: app.cfg.bunq_api_url.clone(),
            name: self.name,
        })
    }
}

#[async_trait]
impl Source for Bunq {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(client) = self.client(app) else {
            return Ok(());
        };
        tri!(client.subscribe().await);
        client.snapshots().await
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(client) = self.client(app) else {
            info!("{}: not configured, skipping", self.name);
            return Ok(());
        };
        let _guard = PULL_LOCK.lock().await;
        tri!(client.pull(true).await);
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn router(&self) -> Router<Arc<App>> {
        let name = self.name;
        Router::new().route(
            &format!("/{name}/notify/{{token}}"),
            post(
                async move |State(app): State<Arc<App>>,
                            Path(token): Path<String>,
                            body: String| {
                    let expected: Option<String> = app.store.get(&format!("{name}/notify_token"));
                    if expected.as_deref() != Some(token.as_str()) {
                        warn!("{name}: notification with a wrong token, ignored");
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    let received = Utc::now();
                    info!("{name}: notification ({} bytes)", body.len());
                    let tracker = app.tasks.clone();
                    drop(tracker.spawn(async move {
                        let Some(client) = Self { name }.client(&app) else {
                            return;
                        };
                        if let Err(error) = client.notification(received, body).await {
                            warn!("{name}: notification handling failed: {error:#}");
                        }
                    }));
                    StatusCode::OK.into_response()
                },
            )
            .head(async || StatusCode::OK),
        )
    }
}

impl Client<'_> {
    /// The lists to pull: the user lists plus the account lists of every monetary account.
    async fn feeds(&self, user: i64) -> Result<Vec<Feed>> {
        let mut feeds: Vec<Feed> = USER_LISTS
            .iter()
            .map(|&(kind, segment)| Feed {
                account: None,
                key: kind.to_owned(),
                kind,
                path: format!("/v1/user/{user}/{segment}"),
            })
            .collect();
        sleep(Duration::from_millis(PACE_MS)).await;
        let (_, accounts, _) = tri!(
            self.get(&format!("/v1/user/{user}/monetary-account?count={PAGE}"))
                .await
        );
        for id in accounts
            .get("Response")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(item_id)
        {
            for &(kind, segment) in ACCOUNT_LISTS {
                feeds.push(Feed {
                    account: Some(id),
                    key: format!("{kind}/{id}"),
                    kind,
                    path: format!("/v1/user/{user}/monetary-account/{id}/{segment}"),
                });
            }
        }
        Ok(feeds)
    }

    /// Commit the gathered files when a chunk is full or at the end, then move the cursors.
    async fn flush(&self, pull: &mut Pull, last: bool) -> Result<()> {
        if !last && pull.files.len() < PAGE_CHUNK {
            return Ok(());
        }
        if !pull.files.is_empty() {
            let message = format!("{}: {} pages", self.name, pull.files.len());
            drop(tri!(self.app.save(&pull.files, &message).await));
            pull.files.clear();
        }
        for (key, newest) in pull.newest.drain(..) {
            let cursor_key = self.key(&format!("cursor/{key}"));
            let cursor: Option<i64> = self.app.store.get(&cursor_key);
            if cursor.is_none_or(|current| newest > current) {
                tri!(self.app.store.set(&cursor_key, &newest));
            }
        }
        for (key, older) in pull.older.drain(..) {
            let older_key = self.key(&format!("older/{key}"));
            match older {
                Some(url) => tri!(self.app.store.set(&older_key, &url)),
                None => tri!(self.app.store.remove(&older_key)),
            }
        }
        Ok(())
    }

    /// GET with the session token. An expired session is renewed once.
    async fn get(&self, path: &str) -> Result<(Session, Value, String)> {
        let mut session = tri!(self.session(false).await);
        let mut renewed = false;
        loop {
            let (status, text) = tri!(
                self.request(Method::GET, path, Some(&session.token), None)
                    .await
            );
            if status == StatusCode::UNAUTHORIZED && !renewed {
                info!("bunq: session rejected, starting a new one");
                session = tri!(self.session(true).await);
                renewed = true;
                continue;
            }
            if !status.is_success() {
                bail!("bunq GET {path} {status}: {text}");
            }
            let value: Value = tri!(serde_json::from_str(&text).context("bunq response"));
            return Ok((session, value, text));
        }
    }

    /// The installation for the current API key, created on first use or after a key rotation.
    async fn installation(&self) -> Result<Installation> {
        let digest = Sha256::digest(self.api_key.as_bytes());
        let hash = format!("{digest:x}");
        if let Some(stored) = self
            .app
            .store
            .get::<Installation>(&self.key("installation_secret"))
        {
            if stored.api_key_sha256 == hash {
                return Ok(stored);
            }
            info!(
                "{}: API key changed, registering a new installation",
                self.name
            );
        }
        let generated =
            tri!(spawn_blocking(|| RsaPrivateKey::new(&mut rand_core::OsRng, KEY_BITS)).await);
        let private = tri!(generated.context("generate RSA key"));
        let public = tri!(
            RsaPublicKey::from(&private)
                .to_public_key_pem(LineEnding::LF)
                .context("encode public key")
        );
        let signer = SigningKey::<Sha256>::new(private.clone());
        let body = tri!(serde_json::to_string(&json!({"client_public_key": public})));
        let (status, text) = tri!(
            self.request(
                Method::POST,
                "/v1/installation",
                None,
                Some((&body, &signer)),
            )
            .await
        );
        if !status.is_success() {
            bail!("bunq installation {status}: {text}");
        }
        let answer: Value = tri!(serde_json::from_str(&text));
        let token = tri!(
            find(&answer, "Token")
                .get("token")
                .and_then(Value::as_str)
                .context("installation token")
        )
        .to_owned();
        let server_public_key = find(&answer, "ServerPublicKey")
            .get("server_public_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let device = tri!(serde_json::to_string(&json!({
            "description": "personal-data collector",
            "secret": self.api_key,
            "permitted_ips": ["*"],
        })));
        let (device_status, device_text) = tri!(
            self.request(
                Method::POST,
                "/v1/device-server",
                Some(&token),
                Some((&device, &signer)),
            )
            .await
        );
        if !device_status.is_success() {
            bail!("bunq device-server {device_status}: {device_text}");
        }
        let installation = Installation {
            api_key_sha256: hash,
            private_key: tri!(
                private
                    .to_pkcs8_pem(LineEnding::LF)
                    .context("encode private key")
            )
            .to_string(),
            server_public_key,
            token,
        };
        tri!(
            self.app
                .store
                .set(&self.key("installation_secret"), &installation)
        );
        tri!(self.app.store.remove(&self.key("session_token")));
        info!("{}: installation and device registered", self.name);
        Ok(installation)
    }

    /// Store key for this user, for example `bunq/cursor/payment/7`.
    fn key(&self, suffix: &str) -> String {
        format!("{}/{suffix}", self.name)
    }

    /// Store a notification and pull the events it announces.
    async fn notification(&self, received: DateTime<Utc>, body: String) -> Result<()> {
        let _guard = PULL_LOCK.lock().await;
        let content = match serde_json::from_str::<&RawValue>(&body) {
            Ok(raw) => tri!(serde_json::to_string(
                &json!({"received_at": received, "body": raw})
            )),
            Err(_) => tri!(serde_json::to_string(
                &json!({"received_at": received, "text": body})
            )),
        };
        let file = File {
            path: storage::path(self.name, "notification", received, None, "json"),
            content,
        };
        let message = format!("{}: notification", self.name);
        drop(tri!(self.app.save(&[file], &message).await));
        tri!(self.pull(false).await);
        Ok(())
    }

    /// Fetch one page, keep it as a file when it has items and track the newest id.
    async fn page(&self, feed: &Feed, url: &str, pull: &mut Pull) -> Result<Value> {
        sleep(Duration::from_millis(PACE_MS)).await;
        let now = Utc::now();
        let (_, value, text) = tri!(self.get(url).await);
        let items = value.get("Response").unwrap_or(&Value::Null);
        if count(items) == 0 {
            return Ok(value);
        }
        if let Some(newest) = items
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(item_id)
            .max()
        {
            pull.newest.push((feed.key.clone(), newest));
        }
        let suffix = feed.account.map(|account| account.to_string());
        pull.files.push(File {
            path: storage::path(self.name, feed.kind, now, suffix.as_deref(), "json"),
            content: tri!(storage::envelope(
                now,
                json!({"url": url, "monetary_account_id": feed.account}),
                &text,
            )),
        });
        info!("{}: {} items from {url}", self.name, count(items));
        Ok(value)
    }

    /// Store every item newer than each cursor. With `backfill`, also continue towards older items.
    async fn pull(&self, backfill: bool) -> Result<()> {
        let session = tri!(self.session(false).await);
        let mut pull = Pull::default();
        let mut pages = 0_usize;
        for feed in tri!(self.feeds(session.user_id).await) {
            if pages >= PAGE_CAP {
                break;
            }
            tri!(self.pull_feed(&feed, backfill, &mut pull, &mut pages).await);
        }
        self.flush(&mut pull, true).await
    }

    /// Pull one list: the rest of its backfill, then everything newer than its cursor.
    async fn pull_feed(
        &self,
        feed: &Feed,
        backfill: bool,
        pull: &mut Pull,
        pages: &mut usize,
    ) -> Result<()> {
        let cursor_key = self.key(&format!("cursor/{}", feed.key));
        let older_key = self.key(&format!("older/{}", feed.key));
        let cursor: Option<i64> = self.app.store.get(&cursor_key);
        let mut older: Option<String> = match cursor {
            None => Some(format!("{}?count={PAGE}", feed.path)),
            Some(_) if backfill => self.app.store.get(&older_key),
            Some(_) => None,
        };
        while let Some(url) = older {
            if *pages >= PAGE_CAP {
                pull.older.push((feed.key.clone(), Some(url)));
                return Ok(());
            }
            let page = tri!(self.page(feed, &url, pull).await);
            *pages = pages.saturating_add(1);
            older = next_url(&page, "older_url");
            // Record where to continue with every commit, so an interrupted run resumes.
            pull.older.push((feed.key.clone(), older.clone()));
            tri!(self.flush(pull, false).await);
        }
        let mut newer = cursor.map(|id| format!("{}?count={PAGE}&newer_id={id}", feed.path));
        while let Some(url) = newer {
            if *pages >= PAGE_CAP {
                return Ok(());
            }
            let page = tri!(self.page(feed, &url, pull).await);
            *pages = pages.saturating_add(1);
            newer = next_url(&page, "newer_url");
            tri!(self.flush(pull, false).await);
        }
        Ok(())
    }

    /// One request with the bunq headers. Bodies are signed. A 429 waits and retries once.
    async fn request(
        &self,
        method: Method,
        path: &str,
        auth: Option<&str>,
        body: Option<(&str, &SigningKey<Sha256>)>,
    ) -> Result<(StatusCode, String)> {
        let url = format!("{}{path}", self.base);
        let mut retried = false;
        loop {
            let mut request = self
                .app
                .http
                .request(method.clone(), &url)
                .header("Cache-Control", "no-cache")
                .header("X-Bunq-Client-Request-Id", Uuid::new_v4().to_string())
                .header("X-Bunq-Geolocation", "0 0 0 0 000")
                .header("X-Bunq-Language", "en_US")
                .header("X-Bunq-Region", "nl_NL");
            if let Some(token) = auth {
                request = request.header("X-Bunq-Client-Authentication", token);
            }
            if let Some((payload, signer)) = body {
                // SHA-256 of the body, signed with PKCS#1 v1.5 and base64 encoded, as bunq expects.
                let signature = BASE64_STANDARD.encode(signer.sign(payload.as_bytes()).to_bytes());
                request = request
                    .header("Content-Type", "application/json")
                    .header("X-Bunq-Client-Signature", signature)
                    .body(payload.to_owned());
            }
            let response = tri!(request.send().await);
            let status = response.status();
            let text = tri!(response.text().await);
            if status == StatusCode::TOO_MANY_REQUESTS && !retried {
                warn!("bunq: rate limited on {path}, waiting");
                sleep(Duration::from_secs(5)).await;
                retried = true;
                continue;
            }
            return Ok((status, text));
        }
    }

    /// A session token with time left, else a fresh one.
    async fn session(&self, fresh: bool) -> Result<Session> {
        if !fresh {
            let stored: Option<Session> = self.app.store.get(&self.key("session_token"));
            let live = stored.filter(|session| {
                session.expires_at.signed_duration_since(Utc::now()) > TimeDelta::minutes(5)
            });
            if let Some(session) = live {
                return Ok(session);
            }
        }
        let installation = tri!(self.installation().await);
        let private = tri!(
            RsaPrivateKey::from_pkcs8_pem(&installation.private_key).context("decode private key")
        );
        let signer = SigningKey::<Sha256>::new(private);
        let body = tri!(serde_json::to_string(&json!({"secret": self.api_key})));
        let (status, text) = tri!(
            self.request(
                Method::POST,
                "/v1/session-server",
                Some(&installation.token),
                Some((&body, &signer)),
            )
            .await
        );
        if !status.is_success() {
            bail!("bunq session-server {status}: {text}");
        }
        let answer: Value = tri!(serde_json::from_str(&text));
        let token = tri!(
            find(&answer, "Token")
                .get("token")
                .and_then(Value::as_str)
                .context("session token")
        )
        .to_owned();
        let user = tri!(
            ["UserPerson", "UserCompany", "UserApiKey"]
                .iter()
                .map(|kind| find(&answer, kind))
                .find(|found| !found.is_null())
                .context("session user")
        );
        let user_id = tri!(
            user.get("id")
                .and_then(Value::as_i64)
                .context("session user id")
        );
        let timeout = user
            .get("session_timeout")
            .and_then(Value::as_i64)
            .unwrap_or(3600);
        let expires_at = tri!(
            TimeDelta::try_seconds(timeout)
                .and_then(|ttl| Utc::now().checked_add_signed(ttl))
                .context("session timeout out of range")
        );
        let session = Session {
            expires_at,
            token,
            user_id,
        };
        tri!(self.app.store.set(&self.key("session_token"), &session));
        info!(
            "{}: session for user {user_id}, valid {timeout} s",
            self.name
        );
        Ok(session)
    }

    /// Daily snapshots of all monetary accounts (with balances) and cards.
    async fn snapshots(&self) -> Result<()> {
        let session = tri!(self.session(false).await);
        let user = session.user_id;
        let mut files = Vec::new();
        for (kind, path) in [
            (
                "monetary_account",
                format!("/v1/user/{user}/monetary-account?count={PAGE}"),
            ),
            ("card", format!("/v1/user/{user}/card?count={PAGE}")),
        ] {
            sleep(Duration::from_millis(PACE_MS)).await;
            let now = Utc::now();
            let (_, value, text) = tri!(self.get(&path).await);
            if value.get("Response").is_none_or(|items| count(items) == 0) {
                continue;
            }
            files.push(File {
                path: storage::path(self.name, kind, now, None, "json"),
                content: tri!(storage::envelope(now, json!({"url": path}), &text)),
            });
        }
        let message = format!("{}: daily snapshot", self.name);
        drop(tri!(self.app.save(&files, &message).await));
        Ok(())
    }

    /// Register the callback for balance changes, keeping any filters set elsewhere. Idempotent.
    async fn subscribe(&self) -> Result<()> {
        let token = if let Some(stored) = self.app.store.get::<String>(&self.key("notify_token")) {
            stored
        } else {
            let first: u128 = rand::rng().random();
            let second: u128 = rand::rng().random();
            let minted = format!("{first:032x}{second:032x}");
            tri!(self.app.store.set(&self.key("notify_token"), &minted));
            minted
        };
        let target = format!("{}/{}/notify/{token}", self.app.cfg.public_url, self.name);
        let session = tri!(self.session(false).await);
        let path = format!("/v1/user/{}/notification-filter-url", session.user_id);
        let (live, current, _) = tri!(self.get(&path).await);
        let mut filters: Vec<Value> = current
            .get("Response")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| item.get("NotificationFilterUrl"))
            .map(|filter| {
                json!({"category": filter.get("category"), "notification_target": filter.get("notification_target")})
            })
            .collect();
        let registered = filters.iter().any(|filter| {
            filter
                .get("category")
                .is_some_and(|category| category == CATEGORY)
                && filter
                    .get("notification_target")
                    .is_some_and(|url| url == target.as_str())
        });
        if registered {
            return Ok(());
        }
        filters.push(json!({"category": CATEGORY, "notification_target": target}));
        let installation = tri!(self.installation().await);
        let private = tri!(
            RsaPrivateKey::from_pkcs8_pem(&installation.private_key).context("decode private key")
        );
        let signer = SigningKey::<Sha256>::new(private);
        let body = tri!(serde_json::to_string(
            &json!({"notification_filters": filters})
        ));
        let (status, text) = tri!(
            self.request(
                Method::POST,
                &path,
                Some(&live.token),
                Some((&body, &signer)),
            )
            .await
        );
        if !status.is_success() {
            bail!("bunq notification-filter-url {status}: {text}");
        }
        info!("{}: callback registered for {CATEGORY}", self.name);
        Ok(())
    }
}

/// The object under `key` in a bunq `Response` array, or null.
fn find<'value>(answer: &'value Value, key: &str) -> &'value Value {
    answer
        .get("Response")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|item| item.get(key))
        .unwrap_or(&Value::Null)
}

/// The id of a list item, which bunq wraps in its type name: `{"Payment": {"id": 1, ...}}`.
fn item_id(item: &Value) -> Option<i64> {
    item.as_object()
        .and_then(|wrapper| wrapper.values().next())
        .and_then(|inner| inner.get("id"))
        .and_then(Value::as_i64)
}

/// The `older_url` or `newer_url` of a page, when the page had items.
fn next_url(page: &Value, direction: &str) -> Option<String> {
    page.pointer(&format!("/Pagination/{direction}"))
        .and_then(Value::as_str)
        .filter(|_| page.get("Response").is_some_and(|items| count(items) > 0))
        .map(String::from)
}
