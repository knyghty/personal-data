//! Store the mail that arrives in the owner's Gmail accounts as metadata: who wrote, to whom, when, the subject and the
//! labels. Bodies are stored only for the senders in `GMAIL_FULL_SENDERS`, such as shops and railways whose mail
//! is a receipt or a ticket, and never for anyone else.
//!
//! Each account is approved once at `/gmail/auth?key=<ADMIN_SECRET>&account=<label>`, with the Google OAuth client
//! of the `google` source. The first runs walk the whole mailbox, newest first, 500 ids a page and a few pages a run.
//! After that each hourly run asks the history API what arrived since the stored history id. Google forgets history
//! ids after about a week; then the run lists the newest page again and starts from the mailbox's current id.
//! Each run commits one file per account with every message it fetched. The daily job stores the labels and the
//! profile (message and thread counts) when they changed.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use core::time::Duration;
use std::collections::HashMap;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Redirect, Response};
use axum::routing::get;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::stream::{self, StreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

use super::{Source, steam, withings};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Store key of the approved account labels.
const ACCOUNTS: &str = "gmail/accounts";
/// Google's authorization endpoint.
const AUTHORIZE: &str = "https://accounts.google.com/o/oauth2/v2/auth";
/// The headers kept of a message that is not stored whole.
const HEADERS: [&str; 7] = [
    "From",
    "To",
    "Cc",
    "Subject",
    "Date",
    "List-Id",
    "Message-ID",
];
/// Metadata requests in flight at once, each followed by `PACE`.
///
/// A get costs 5 of the 15,000 quota units a minute Google allows per user, so this stays around 100 units a second.
const IN_FLIGHT: usize = 2;
/// Messages fetched per account per run; the rest waits for the next run.
const MAX_MESSAGES: usize = 2_000;
/// Store key of the approval in progress.
const OAUTH_STATE: &str = "gmail/oauth_state";
/// Pause after each metadata request.
const PACE: Duration = Duration::from_millis(100);
/// Ids per list or history page, the API's maximum.
const PAGE: usize = 500;
/// Pause before retrying a quota refusal (429, or 403 with a quota message), times the attempt: the quota is per minute.
const QUOTA_PAUSE_MS: u64 = if cfg!(test) { 10 } else { 10_000 };
/// Retries of a refused or failed request.
const RETRIES: u32 = 4;
/// Pause before retrying a 5xx, times the attempt.
const RETRY_PAUSE_MS: u64 = 500;
/// The scope asked for: read-only mail access.
const SCOPE: &str = "https://www.googleapis.com/auth/gmail.readonly";
/// Directory under `raw/` and prefix of the store keys.
const SOURCE: &str = "gmail";
/// Ask to approve again this long before access ends; only a grant from an app in testing has an end.
const WARN_BEFORE: TimeDelta = TimeDelta::hours(24);

/// An approval in progress: the OAuth state and the account it is for.
#[derive(Serialize, Deserialize)]
struct AuthState {
    /// The label the grant is stored under.
    account: String,
    /// The random state the callback must echo.
    state: String,
}

/// Where a mailbox walk stands: the next page of the first walk, then the history id to continue from.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq, Debug)]
struct Cursor {
    /// Whether the first walk reached the oldest message.
    #[serde(default)]
    backfilled: bool,
    /// The history id the next run continues from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_id: Option<String>,
    /// The next page of the first walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
}

/// The source: every approved account.
pub(super) struct Gmail;

/// A refresh token and when it stops working.
#[derive(Serialize, Deserialize)]
struct Grant {
    /// When the refresh token stops working; unknown for a grant without an end.
    expires_at: Option<DateTime<Utc>>,
    /// The token that mints access tokens.
    refresh_token: String,
}

/// One approved account with an access token for this run.
struct Mailbox<'app> {
    /// The label the account was approved under.
    account: &'app str,
    /// The shared clients and configuration.
    app: &'app App,
    /// When the grant ends, for the warning before it does.
    expires_at: Option<DateTime<Utc>>,
    /// The access token.
    token: String,
}

/// The query string of the OAuth routes.
type Params = Query<HashMap<String, String>>;

impl AuthState {
    /// Exchange the callback's code for a grant, store it and list the account.
    ///
    /// A new grant starts the mailbox walk over.
    async fn finish(&self, app: &App, code: &str) -> Response {
        let exchange = token_request(
            app,
            &[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", &redirect_uri(app)),
            ],
        )
        .await;
        let answer = match exchange {
            Ok(answer) => answer,
            Err(error) => return reply(StatusCode::BAD_GATEWAY, &format!("{error:#}")),
        };
        let scopes = answer
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !scopes.split(' ').any(|scope| scope == SCOPE) {
            return reply(
                StatusCode::BAD_GATEWAY,
                &format!("the grant lacks {SCOPE}; tick the mail scope when approving"),
            );
        }
        let Some(refresh_token) = answer.get("refresh_token").and_then(Value::as_str) else {
            return reply(
                StatusCode::BAD_GATEWAY,
                "no refresh_token; approve again with prompt=consent",
            );
        };
        let grant = Grant {
            expires_at: answer
                .get("refresh_token_expires_in")
                .and_then(Value::as_i64)
                .and_then(TimeDelta::try_seconds)
                .and_then(|ttl| Utc::now().checked_add_signed(ttl)),
            refresh_token: refresh_token.to_owned(),
        };
        let mut labels = accounts(app);
        let listed = labels.contains(&self.account);
        if !listed {
            labels.push(self.account.clone());
        }
        let stored = app
            .store
            .set(&key(&self.account, "oauth"), &grant)
            .and_then(|()| {
                if listed {
                    Ok(())
                } else {
                    app.store.set(ACCOUNTS, &labels)
                }
            });
        if let Err(error) = stored {
            return reply(StatusCode::BAD_GATEWAY, &format!("{error:#}"));
        }
        if let Err(error) = app.store.remove(OAUTH_STATE) {
            warn!("gmail: forget the approval state: {error:#}");
        }
        let until = grant
            .expires_at
            .map_or_else(|| "no end date".to_owned(), |end| format!("until {end}"));
        format!(
            "Gmail account {} connected ({until}). The next run starts reading it.\n",
            self.account
        )
        .into_response()
    }

    /// Store the pending state and send the browser to Google.
    fn redirect(&self, app: &App, client_id: &str) -> Response {
        if let Err(error) = app.store.set(OAUTH_STATE, self) {
            return reply(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}"));
        }
        let params = [
            ("client_id", client_id),
            ("redirect_uri", &redirect_uri(app)),
            ("response_type", "code"),
            ("access_type", "offline"),
            ("prompt", "consent"),
            ("scope", SCOPE),
            ("state", &self.state),
        ];
        match Url::parse_with_params(AUTHORIZE, params) {
            Ok(url) => Redirect::temporary(url.as_str()).into_response(),
            Err(error) => reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("authorize url: {error}"),
            ),
        }
    }
}

#[async_trait]
impl Source for Gmail {
    async fn daily(&self, app: &App) -> Result<()> {
        let mut problems = Vec::new();
        for account in accounts(app) {
            let result = match Mailbox::open(app, &account).await {
                Ok(mailbox) => mailbox.snapshots().await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                problems.push(format!("{account}: {error:#}"));
            }
        }
        report(&problems)
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let mut problems = Vec::new();
        for account in accounts(app) {
            let result = match Mailbox::open(app, &account).await {
                Ok(mailbox) => mailbox.run().await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                problems.push(format!("{account}: {error:#}"));
            }
        }
        report(&problems)
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
            .route(
                "/gmail/auth",
                get(async |State(app): State<Arc<App>>, Query(query): Params| {
                    if query.get("key") != Some(&app.cfg.admin_secret) {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    let Some(account) = query
                        .get("account")
                        .map(String::as_str)
                        .map(storage::slug)
                        .filter(|label| !label.is_empty())
                    else {
                        return reply(
                            StatusCode::BAD_REQUEST,
                            "add account=<label>, e.g. account=personal",
                        );
                    };
                    let Ok((client_id, _)) = client(&app) else {
                        return reply(
                            StatusCode::BAD_REQUEST,
                            "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET",
                        );
                    };
                    let pending = AuthState {
                        account,
                        state: withings::random_state(),
                    };
                    pending.redirect(&app, client_id)
                }),
            )
            .route(
                "/gmail/callback",
                get(async |State(app): State<Arc<App>>, Query(query): Params| {
                    if let Some(error) = query.get("error") {
                        return reply(
                            StatusCode::BAD_REQUEST,
                            &format!("Google returned an error: {error}"),
                        );
                    }
                    let stored: Option<AuthState> = app.store.get(OAUTH_STATE);
                    let Some(pending) =
                        stored.filter(|saved| query.get("state") == Some(&saved.state))
                    else {
                        return reply(
                            StatusCode::BAD_REQUEST,
                            "state mismatch, start again at /gmail/auth",
                        );
                    };
                    let Some(code) = query.get("code") else {
                        return reply(StatusCode::BAD_REQUEST, "missing code");
                    };
                    pending.finish(&app, code).await
                }),
            )
    }
}

impl<'app> Mailbox<'app> {
    /// Fetch the messages with these ids: metadata without the snippet, or whole for an allowlisted sender.
    ///
    /// A message deleted since it was listed is skipped.
    async fn fetch(&self, ids: &[String]) -> Result<Vec<Value>> {
        let mut params: Vec<(&str, &str)> = vec![("format", "metadata")];
        params.extend(
            HEADERS
                .iter()
                .copied()
                .map(|header| ("metadataHeaders", header)),
        );
        let requests: Vec<_> = ids.iter().map(|id| self.fetch_one(id, &params)).collect();
        let results: Vec<Result<Option<Value>>> = stream::iter(requests)
            .buffer_unordered(IN_FLIGHT)
            .collect()
            .await;
        let mut messages: Vec<Value> = tri!(results.into_iter().collect::<Result<Vec<_>>>())
            .into_iter()
            .flatten()
            .collect();
        // Concurrency returns messages in any order; sort by id so files are stable.
        messages.sort_by(|left, right| {
            let left_id = left.get("id").and_then(Value::as_str);
            left_id.cmp(&right.get("id").and_then(Value::as_str))
        });
        Ok(messages)
    }

    /// Fetch one message as `fetch` stores it; `None` when it is gone.
    async fn fetch_one(&self, id: &str, metadata: &[(&str, &str)]) -> Result<Option<Value>> {
        let path = format!("users/me/messages/{id}");
        let (text, status) = tri!(self.get(&path, metadata).await);
        sleep(PACE).await;
        if status == StatusCode::NOT_FOUND {
            warn!("gmail: message {id} is gone");
            return Ok(None);
        }
        let mut message: Value = tri!(serde_json::from_str(&text));
        if self.stores_body(&message) {
            let (full_text, full_status) = tri!(self.get(&path, &[("format", "full")]).await);
            if full_status != StatusCode::NOT_FOUND {
                return Ok(Some(tri!(serde_json::from_str(&full_text))));
            }
        }
        if let Some(fields) = message.as_object_mut() {
            drop(fields.remove("snippet"));
        }
        Ok(Some(message))
    }

    /// GET a Gmail API path, retrying 429, 5xx and the 403 Google sends for an exceeded quota.
    ///
    /// Return the body and status; 404 is returned for the caller to judge, any other failure is an error.
    async fn get(&self, path: &str, params: &[(&str, &str)]) -> Result<(String, StatusCode)> {
        let url = tri!(Url::parse_with_params(
            &format!("{}/{path}", self.app.cfg.gmail_api_url),
            params
        ));
        let mut attempt = 0_u32;
        loop {
            let response = tri!(
                self.app
                    .http
                    .get(url.clone())
                    .bearer_auth(&self.token)
                    .send()
                    .await
            );
            let status = response.status();
            let text = tri!(response.text().await);
            if status.is_success() || status == StatusCode::NOT_FOUND {
                return Ok((text, status));
            }
            let message = format!("GET {path}: {status} {}", snippet(&text));
            let quota = status == StatusCode::TOO_MANY_REQUESTS
                || (status == StatusCode::FORBIDDEN
                    && (text.contains("Quota exceeded")
                        || text.to_ascii_lowercase().contains("ratelimitexceeded")));
            if !(quota || status.is_server_error()) || attempt == RETRIES {
                bail!(message);
            }
            attempt = attempt.saturating_add(1);
            warn!("gmail: {message}, retrying");
            let pause = if quota {
                QUOTA_PAUSE_MS
            } else {
                RETRY_PAUSE_MS
            };
            sleep(Duration::from_millis(
                pause.saturating_mul(u64::from(attempt)),
            ))
            .await;
        }
    }

    /// List the ids of the messages added since the cursor's history id, which moves to the last record read.
    ///
    /// Stop after `MAX_MESSAGES` ids, so a long backlog takes several runs. `None` when Google forgot the id.
    async fn history(&self, cursor: &mut Cursor) -> Result<Option<Vec<String>>> {
        let start = tri!(cursor.history_id.clone().context("no history id"));
        let mut ids = BTreeSet::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut params = vec![
                ("startHistoryId", start.as_str()),
                ("historyTypes", "messageAdded"),
                ("maxResults", "500"),
            ];
            if let Some(token) = page_token.as_deref() {
                params.push(("pageToken", token));
            }
            let (text, status) = tri!(self.get("users/me/history", &params).await);
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let page: Value = tri!(serde_json::from_str(&text));
            let mut last = None;
            for record in page
                .get("history")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                for added in record
                    .get("messagesAdded")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(id) = added.pointer("/message/id").and_then(Value::as_str) {
                        let _: bool = ids.insert(id.to_owned());
                    }
                }
                last = record.get("id").and_then(Value::as_str).map(String::from);
                if ids.len() >= MAX_MESSAGES {
                    break;
                }
            }
            page_token = page
                .get("nextPageToken")
                .and_then(Value::as_str)
                .map(String::from);
            if ids.len() >= MAX_MESSAGES && page_token.is_some() {
                cursor.history_id = last;
                break;
            }
            if page_token.is_none() {
                if let Some(id) = page.get("historyId").and_then(Value::as_str) {
                    cursor.history_id = Some(id.to_owned());
                }
                break;
            }
        }
        Ok(Some(ids.into_iter().collect()))
    }

    /// List one page of message ids, newest first, and the token of the next page.
    async fn list(&self, page_token: Option<&str>) -> Result<(Vec<String>, Option<String>)> {
        let max = PAGE.to_string();
        let mut params = vec![("maxResults", max.as_str()), ("includeSpamTrash", "false")];
        if let Some(token) = page_token {
            params.push(("pageToken", token));
        }
        let (text, _) = tri!(self.get("users/me/messages", &params).await);
        let page: Value = tri!(serde_json::from_str(&text));
        let ids = page
            .get("messages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|message| message.get("id").and_then(Value::as_str).map(String::from))
            .collect();
        let next = page
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(String::from);
        Ok((ids, next))
    }

    /// Mint an access token for an approved account.
    async fn open(app: &'app App, account: &'app str) -> Result<Self> {
        let Some(grant) = app.store.get::<Grant>(&key(account, "oauth")) else {
            bail!("not approved; {}", approve_hint(app, account));
        };
        let answer = tri!(
            token_request(
                app,
                &[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", &grant.refresh_token),
                ],
            )
            .await
            .with_context(|| approve_hint(app, account))
        );
        let token = tri!(
            answer
                .get("access_token")
                .and_then(Value::as_str)
                .context("access_token")
        )
        .to_owned();
        Ok(Self {
            account,
            app,
            expires_at: grant.expires_at,
            token,
        })
    }

    /// Collect the ids to fetch this run and move the cursor past them.
    ///
    /// A backfilled mailbox follows the history API; the first runs walk the mailbox page by page.
    async fn pending(&self, cursor: &mut Cursor) -> Result<Vec<String>> {
        if cursor.backfilled {
            if let Some(ids) = tri!(self.history(cursor).await) {
                return Ok(ids);
            }
            warn!(
                "gmail: {} history id forgotten, listing the newest page",
                self.account
            );
            cursor.history_id = Some(tri!(self.profile().await));
            let (ids, _) = tri!(self.list(None).await);
            return Ok(ids);
        }
        if cursor.history_id.is_none() {
            // The first walk takes several runs; history continues from where the mailbox stood when it began.
            cursor.history_id = Some(tri!(self.profile().await));
        }
        let mut ids = Vec::new();
        while ids.len() < MAX_MESSAGES {
            let (page, next) = tri!(self.list(cursor.page_token.as_deref()).await);
            ids.extend(page);
            cursor.page_token = next;
            if cursor.page_token.is_none() {
                cursor.backfilled = true;
                break;
            }
        }
        Ok(ids)
    }

    /// Read the mailbox's current history id.
    async fn profile(&self) -> Result<String> {
        let (text, _) = tri!(self.get("users/me/profile", &[]).await);
        let profile: Value = tri!(serde_json::from_str(&text));
        let history_id = profile.get("historyId");
        history_id
            .and_then(Value::as_str)
            .map(String::from)
            .or_else(|| history_id.and_then(Value::as_u64).map(|id| id.to_string()))
            .context("profile without historyId")
    }

    /// Fetch and commit what arrived since the cursor.
    async fn run(&self) -> Result<()> {
        let cursor_key = key(self.account, "cursor");
        let mut cursor: Cursor = self.app.store.get(&cursor_key).unwrap_or_default();
        let before = cursor.clone();
        let ids = tri!(self.pending(&mut cursor).await);
        let messages = tri!(self.fetch(&ids).await);
        if !messages.is_empty() {
            let now = Utc::now();
            let request = json!({
                "account": self.account,
                "count": messages.len(),
                "from_history_id": before.history_id,
                "history_id": cursor.history_id,
                "page_token": before.page_token,
            });
            let file = File {
                path: storage::path(
                    SOURCE,
                    &format!("{}/message", self.account),
                    now,
                    None,
                    "json",
                ),
                content: tri!(storage::envelope(
                    now,
                    request,
                    &tri!(serde_json::to_string(&messages))
                )),
            };
            let message = format!("gmail: {} ({})", self.account, messages.len());
            drop(tri!(self.app.save(&[file], &message).await));
            info!("gmail: {message}");
        }
        if cursor != before {
            tri!(self.app.store.set(&cursor_key, &cursor));
        }
        let ending = self
            .expires_at
            .filter(|end| end.signed_duration_since(Utc::now()) < WARN_BEFORE);
        if let Some(end) = ending {
            bail!(
                "access ends {end}; {}",
                approve_hint(self.app, self.account)
            );
        }
        Ok(())
    }

    /// Store the labels and the profile when they changed since the last daily job.
    async fn snapshots(&self) -> Result<()> {
        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for what in ["labels", "profile"] {
            let (text, _) = tri!(self.get(&format!("users/me/{what}"), &[]).await);
            let sha_key = key(self.account, &format!("sha/{what}"));
            if let Some(sha) = steam::changed(self.app, &sha_key, &text) {
                files.push(File {
                    path: storage::path(
                        SOURCE,
                        &format!("{}/{what}", self.account),
                        now,
                        None,
                        "json",
                    ),
                    content: tri!(storage::envelope(
                        now,
                        json!({"account": self.account}),
                        &text
                    )),
                });
                hashes.push((sha_key, sha));
            }
        }
        if files.is_empty() {
            return Ok(());
        }
        let message = format!("gmail: {} labels, profile", self.account);
        drop(tri!(self.app.save(&files, &message).await));
        for (hash_key, sha) in hashes {
            tri!(self.app.store.set(&hash_key, &sha));
        }
        Ok(())
    }

    /// Whether the message's sender is on the allowlist, by address or by domain (subdomains included).
    ///
    /// The address is read from the From header, `Name <addr>` or bare, and lower-cased.
    fn stores_body(&self, message: &Value) -> bool {
        let sender = message
            .pointer("/payload/headers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|header| {
                header
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name.eq_ignore_ascii_case("From"))
            })
            .and_then(|header| header.get("value").and_then(Value::as_str));
        let Some(from) = sender else {
            return false;
        };
        let inner = from
            .rsplit_once('<')
            .and_then(|(_, rest)| rest.rsplit_once('>'))
            .map_or(from, |(inner, _)| inner);
        let address = inner.trim().to_ascii_lowercase();
        if !address.contains('@') {
            return false;
        }
        self.app.cfg.gmail_full_senders.iter().any(|allowed| {
            let wanted = allowed.trim().to_ascii_lowercase();
            !wanted.is_empty()
                && (address == wanted
                    || address.ends_with(&format!("@{wanted}"))
                    || address.ends_with(&format!(".{wanted}")))
        })
    }
}

/// The approved account labels.
fn accounts(app: &App) -> Vec<String> {
    app.store.get(ACCOUNTS).unwrap_or_default()
}

/// Tell where to approve the account again, without the secret.
fn approve_hint(app: &App, account: &str) -> String {
    format!(
        "approve again at {}/gmail/auth?account={account}&key=<ADMIN_SECRET>",
        app.cfg.public_url
    )
}

/// The Google OAuth client id and secret.
///
/// # Errors
///
/// When either is not configured.
fn client(app: &App) -> Result<(&str, &str)> {
    match (
        app.cfg.google_client_id.as_deref(),
        app.cfg.google_client_secret.as_deref(),
    ) {
        (Some(id), Some(secret)) => Ok((id, secret)),
        _ => bail!("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET are not set"),
    }
}

/// Store key of `what` for an account.
fn key(account: &str, what: &str) -> String {
    format!("gmail/{account}/{what}")
}

/// The callback URL registered with Google.
fn redirect_uri(app: &App) -> String {
    format!("{}/gmail/callback", app.cfg.public_url)
}

/// A plain-text response.
fn reply(status: StatusCode, text: &str) -> Response {
    (status, text.to_owned()).into_response()
}

/// Join every account's failure into one error, so the others still ran.
fn report(problems: &[String]) -> Result<()> {
    if problems.is_empty() {
        Ok(())
    } else {
        bail!(problems.join("; "))
    }
}

/// The start of a response body, for an error message.
fn snippet(text: &str) -> String {
    text.chars().take(300).collect()
}

/// POST the token endpoint with the client credentials and `params`.
async fn token_request(app: &App, params: &[(&str, &str)]) -> Result<Value> {
    let (id, secret) = tri!(client(app));
    let mut form = vec![("client_id", id), ("client_secret", secret)];
    form.extend_from_slice(params);
    let response = tri!(
        app.http
            .post(&app.cfg.google_token_url)
            .form(&form)
            .send()
            .await
    );
    let status = response.status();
    let text = tri!(response.text().await);
    if !status.is_success() {
        bail!("token request: {status} {}", snippet(&text));
    }
    Ok(tri!(serde_json::from_str(&text)))
}
