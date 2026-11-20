//! Export `YouTube`, Maps and Search history from Google accounts with the Data Portability API, once a day per account.
//!
//! Each account is approved once at `/google/auth?key=<ADMIN_SECRET>&account=<label>`, a brand account included.
//! Every day two exports start per account: `activity` (My Activity, from the last export onwards) and `snapshot`
//! (subscriptions, playlists, comments, places, ...). Exports take minutes to hours, so hourly runs poll them and
//! commit every file of the finished zip archives. Snapshot files are committed only when they changed.
//!
//! Access lasts 180 days for the published but unverified app, 7 days while it is in testing. A day before it ends,
//! the run fails and asks to approve again, which opens the usual issue.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::fmt::Display;
use core::mem;
use std::collections::HashMap;
use std::io::Read as _;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Redirect, Response};
use axum::routing::get;
use chrono::{DateTime, Duration, Utc};
use flate2::read::DeflateDecoder;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};
use tracing::{info, warn};
use url::{ParseError, Url};

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Store key of the approved account labels.
const ACCOUNTS: &str = "google/accounts";
/// Google's consent page.
const AUTHORIZE: &str = "https://accounts.google.com/o/oauth2/v2/auth";
/// Google allows one export of a resource per 24 hours.
const EXPORT_EVERY: Duration = Duration::hours(24);
/// The archive's own table of contents, which changes with every export.
const INDEX: &str = "archive_browser.json";
/// Largest file to commit in one piece: GitHub refused a 43 MB blob through the API.
const MAX_PART: Limit = Limit(if cfg!(test) { 64 } else { 20 << 20 });
/// Store key of the approval in progress.
const OAUTH_STATE: &str = "google/oauth_state";
/// Activity exports start this long before the previous one ended, so nothing falls between them.
const OVERLAP: Duration = Duration::hours(1);
/// Resources per product. Uploaded videos and Maps photos are left out: they are media, not history.
const PRODUCTS: Products = Products([
    (
        "youtube",
        &[
            "myactivity.youtube",
            "youtube.subscriptions",
            "youtube.comments",
            "youtube.live_chat",
            "youtube.public_playlists",
            "youtube.unlisted_playlists",
            "youtube.private_playlists",
            "youtube.music",
            "youtube.channel",
        ],
    ),
    (
        "maps",
        &[
            "myactivity.maps",
            "maps.starred_places",
            "maps.aliased_places",
            "maps.commute_routes",
            "maps.commute_settings",
            "maps.reviews",
            "maps.factual_contributions",
            "maps.questions_answers",
        ],
    ),
    ("search", &["myactivity.search"]),
]);
/// Google retries a failed export at most three times.
const RETRIES: u32 = 3;
/// Prefix of every Data Portability scope.
const SCOPE_PREFIX: &str = "https://www.googleapis.com/auth/dataportability.";
/// Directory name under `raw/`.
const SOURCE: &str = "google";
/// Ask to approve again this long before access ends.
const WARN_BEFORE: Duration = Duration::hours(24);

/// A zip archive, read from its central directory, so entries with data descriptors work.
/// ponytail: no zip64, so archives over 4 GB fail; Google splits exports into smaller archives.
struct Archive<'data>(&'data [u8]);

impl Archive<'_> {
    /// The files of the archive, by name.
    fn unzip(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let data = self.0;
        let u16_at = |base: usize, field: usize| -> Result<usize> {
            let from = tri!(base.checked_add(field).context("zip truncated"));
            let bytes: Option<[u8; 2]> = from
                .checked_add(2)
                .and_then(|to| data.get(from..to))
                .and_then(|slice| slice.try_into().ok());
            Ok(usize::from(u16::from_le_bytes(tri!(
                bytes.context("zip truncated")
            ))))
        };
        let u32_at = |base: usize, field: usize| -> Result<usize> {
            let from = tri!(base.checked_add(field).context("zip truncated"));
            let bytes: Option<[u8; 4]> = from
                .checked_add(4)
                .and_then(|to| data.get(from..to))
                .and_then(|slice| slice.try_into().ok());
            let value = u32::from_le_bytes(tri!(bytes.context("zip truncated")));
            usize::try_from(value).context("zip offset beyond the address space")
        };
        let magic = [0x50, 0x4b, 0x05, 0x06];
        let end = tri!(
            (0..data.len().saturating_sub(21))
                .rev()
                .take(65_557)
                .find(|&candidate| data
                    .get(candidate..)
                    .is_some_and(|rest| rest.starts_with(&magic)))
                .context("not a zip archive")
        );
        let entries = tri!(u16_at(end, 10));
        let mut entry = tri!(u32_at(end, 16));
        if entry == 0xFFFF_FFFF {
            bail!("zip64 archives are not supported");
        }
        let mut out = Vec::with_capacity(entries);
        for _ in 0..entries {
            if tri!(u32_at(entry, 0)) != 0x0201_4b50 {
                bail!("bad zip central directory");
            }
            let method = tri!(u16_at(entry, 10));
            let size = tri!(u32_at(entry, 20));
            let name_len = tri!(u16_at(entry, 28));
            let extra_len = tri!(u16_at(entry, 30));
            let comment_len = tri!(u16_at(entry, 32));
            let local = tri!(u32_at(entry, 42));
            let name_start = tri!(entry.checked_add(46).context("zip truncated"));
            let name_end = tri!(name_start.checked_add(name_len).context("zip truncated"));
            let name_bytes = tri!(data.get(name_start..name_end).context("zip truncated"));
            let name = String::from_utf8_lossy(name_bytes).into_owned();
            entry = tri!(
                name_end
                    .checked_add(extra_len)
                    .and_then(|after| after.checked_add(comment_len))
                    .context("zip truncated")
            );
            if name.ends_with('/') {
                continue;
            }
            let local_name_len = tri!(u16_at(local, 26));
            let local_extra_len = tri!(u16_at(local, 28));
            let start = tri!(
                local
                    .checked_add(30)
                    .and_then(|header| header.checked_add(local_name_len))
                    .and_then(|header| header.checked_add(local_extra_len))
                    .context("zip truncated")
            );
            let stop = tri!(start.checked_add(size).context("zip truncated"));
            let raw = tri!(data.get(start..stop).context("zip truncated"));
            let bytes = match method {
                0 => raw.to_vec(),
                8 => {
                    let mut inflated = Vec::new();
                    let _: usize = tri!(
                        DeflateDecoder::new(raw)
                            .read_to_end(&mut inflated)
                            .with_context(|| format!("inflate {name}"))
                    );
                    inflated
                }
                other => bail!("{name}: zip method {other} is not supported"),
            };
            out.push((name, bytes));
        }
        Ok(out)
    }
}

/// An approval in progress: the state sent to Google and the account it is for.
#[derive(Serialize, Deserialize)]
struct AuthState {
    /// Account label given at `/google/auth`.
    account: String,
    /// Random state checked on the callback.
    state: String,
}

impl AuthState {
    /// Google's consent link for `scope`, which sends the browser back to the callback.
    fn authorize_url(
        &self,
        client_id: &str,
        scope: &str,
        public_url: &str,
    ) -> Result<Url, ParseError> {
        let redirect_uri = format!("{public_url}/google/callback");
        Url::parse_with_params(
            AUTHORIZE,
            [
                ("client_id", client_id),
                ("redirect_uri", redirect_uri.as_str()),
                ("response_type", "code"),
                ("access_type", "offline"),
                ("prompt", "consent"),
                ("scope", scope),
                ("state", self.state.as_str()),
            ],
        )
    }

    /// Store the refresh token and granted resources of a code exchange, and list the account.
    fn save_grant(&self, app: &App, reply: &Value) -> Result<Grant> {
        let refresh_token = tri!(
            reply
                .get("refresh_token")
                .and_then(Value::as_str)
                .context("no refresh_token; approve again with prompt=consent")
        );
        let grant = Grant {
            expires_at: reply
                .get("refresh_token_expires_in")
                .and_then(Value::as_i64)
                .and_then(Duration::try_seconds)
                .and_then(|lifetime| Utc::now().checked_add_signed(lifetime)),
            refresh_token: refresh_token.to_owned(),
            resources: reply
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .split(' ')
                .filter_map(|scope| scope.strip_prefix(SCOPE_PREFIX))
                .map(String::from)
                .collect(),
        };
        tri!(app.store.set(&key(&self.account, "oauth"), &grant));
        let mut accounts: Vec<String> = app.store.get(ACCOUNTS).unwrap_or_default();
        if !accounts.contains(&self.account) {
            accounts.push(self.account.clone());
            tri!(app.store.set(ACCOUNTS, &accounts));
        }
        Ok(grant)
    }
}

/// The source.
pub(super) struct Google;

#[async_trait]
impl Source for Google {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    /// Poll every account's running exports and start the ones due today; one failing account does not stop the others.
    async fn hourly(&self, app: &App) -> Result<()> {
        let accounts: Vec<String> = app.store.get(ACCOUNTS).unwrap_or_default();
        let mut problems = Vec::new();
        for account in accounts {
            let result: Result<()> = async {
                let Some(grant) = app.store.get::<Grant>(&key(&account, "oauth")) else {
                    bail!("not approved; {}", approve_hint(app, &account));
                };
                let refresh = [
                    ("grant_type", "refresh_token"),
                    ("refresh_token", &grant.refresh_token),
                ];
                let reply = tri!(
                    token_request(app, &refresh)
                        .await
                        .with_context(|| approve_hint(app, &account))
                );
                let token = tri!(text_field(&reply, "access_token")).to_owned();
                let jobs: Vec<Job> = app.store.get(&key(&account, "jobs")).unwrap_or_default();
                // A job that fails to poll or commit stays listed, and the others still run; the first error fails the run.
                let mut next = Vec::new();
                let mut failed = None;
                for job in jobs {
                    let polled = job.clone().poll(app, &account, &token).await;
                    match polled {
                        Ok(Some(running)) => next.push(running),
                        Ok(None) => {}
                        Err(error) => {
                            next.push(job);
                            failed = failed.or(Some(error));
                        }
                    }
                }
                tri!(app.store.set(&key(&account, "jobs"), &next));
                if let Some(error) = failed {
                    return Err(error);
                }
                let now = Utc::now();
                for (kind, resources) in grant.split() {
                    let started_key = key(&account, &format!("started/{kind}"));
                    let last: Option<DateTime<Utc>> = app.store.get(&started_key);
                    let busy = next.iter().any(|job| job.kind == kind);
                    let recent = last
                        .is_some_and(|started| now.signed_duration_since(started) < EXPORT_EVERY);
                    if resources.is_empty() || busy || recent {
                        continue;
                    }
                    let mut body = Map::new();
                    drop(body.insert("resources".to_owned(), json!(resources)));
                    let activity_until: Option<DateTime<Utc>> = (kind == "activity")
                        .then(|| app.store.get(&key(&account, "activity_until")))
                        .flatten();
                    if let Some(since) = activity_until {
                        let start = tri!(
                            since
                                .checked_sub_signed(OVERLAP)
                                .context("activity_until out of range")
                        );
                        drop(body.insert("startTime".to_owned(), json!(start)));
                    }
                    let initiate_url = format!(
                        "{}/v1/portabilityArchive:initiate",
                        app.cfg.google_portability_url
                    );
                    let request = app.http.post(&initiate_url).bearer_auth(&token).json(&body);
                    let resp = tri!(request.send().await);
                    let status = resp.status();
                    let text = tri!(resp.text().await);
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        info!("google: {account} {kind} export not allowed yet: {text}");
                        continue;
                    }
                    if !status.is_success() {
                        bail!("start {kind} export: {status} {}", snippet(&text));
                    }
                    let id = tri!(job_id(&text));
                    info!("google: {account} started {kind} export {id}");
                    next.push(Job {
                        id,
                        kind: kind.to_owned(),
                        retries: 0,
                    });
                    tri!(app.store.set(&key(&account, "jobs"), &next));
                    tri!(app.store.set(&started_key, &now));
                }
                let ending = grant
                    .expires_at
                    .filter(|end| end.signed_duration_since(now) < WARN_BEFORE);
                if let Some(end) = ending {
                    bail!("access ends {end}; {}", approve_hint(app, &account));
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                problems.push(format!("{account}: {error:#}"));
            }
        }
        if !problems.is_empty() {
            bail!(problems.join("; "));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    /// `/google/auth` starts an approval, `/google/callback` finishes it with the code Google sends back.
    fn router(&self) -> Router<Arc<App>> {
        let auth = async |State(app): State<Arc<App>>,
                          Query(query): Query<HashMap<String, String>>|
               -> Response {
            if query.get("key") != Some(&app.cfg.admin_secret) {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let Some(account) = query
                .get("account")
                .map(|label| storage::slug(label))
                .filter(|slug| !slug.is_empty())
            else {
                return (
                    StatusCode::BAD_REQUEST,
                    "add account=<label>, e.g. account=personal",
                )
                    .into_response();
            };
            let (Ok((client_id, _)), Ok(scope)) =
                (client(&app), PRODUCTS.scopes(query.get("products")))
            else {
                return (
                    StatusCode::BAD_REQUEST,
                    "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET, and pick products from youtube, maps, search",
                )
                    .into_response();
            };
            // Google refuses scopes that a live grant already covers, so end the old grant first.
            if let Some(grant) = app.store.get::<Grant>(&key(&account, "oauth")) {
                grant.revoke(&app, &account).await;
            }
            let saved = AuthState {
                account,
                state: super::withings::random_state(),
            };
            if let Err(error) = app.store.set(OAUTH_STATE, &saved) {
                return failed(StatusCode::INTERNAL_SERVER_ERROR, &error);
            }
            let authorize = saved.authorize_url(client_id, &scope, &app.cfg.public_url);
            match authorize {
                Ok(url) => Redirect::temporary(url.as_str()).into_response(),
                Err(error) => failed(StatusCode::INTERNAL_SERVER_ERROR, &error),
            }
        };
        let callback = async |State(app): State<Arc<App>>,
                              Query(query): Query<HashMap<String, String>>|
               -> Response {
            if let Some(error) = query.get("error") {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Google returned an error: {error}"),
                )
                    .into_response();
            }
            let pending: Option<AuthState> = app.store.get(OAUTH_STATE);
            let Some(saved) =
                pending.filter(|auth_state| query.get("state") == Some(&auth_state.state))
            else {
                return (
                    StatusCode::BAD_REQUEST,
                    "state mismatch, start again at /google/auth",
                )
                    .into_response();
            };
            let Some(code) = query.get("code") else {
                return (StatusCode::BAD_REQUEST, "missing code").into_response();
            };
            let redirect = format!("{}/google/callback", app.cfg.public_url);
            let params = [
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", &redirect),
            ];
            let exchanged = token_request(&app, &params).await;
            let reply = match exchanged {
                Ok(reply) => reply,
                Err(error) => return failed(StatusCode::BAD_GATEWAY, &error),
            };
            let stored = saved.save_grant(&app, &reply);
            match stored {
                Ok(grant) => {
                    if let Err(error) = app.store.remove(OAUTH_STATE) {
                        warn!("google: forget the approval state: {error:#}");
                    }
                    let until = grant
                        .expires_at
                        .map_or_else(|| "no end date".to_owned(), |end| format!("until {end}"));
                    format!(
                        "Google account {} connected ({} resources, {until}). The next run starts its exports.\n",
                        saved.account,
                        grant.resources.len()
                    )
                    .into_response()
                }
                Err(error) => failed(StatusCode::BAD_GATEWAY, &error),
            }
        };
        Router::new()
            .route("/google/auth", get(auth))
            .route("/google/callback", get(callback))
    }
}

/// An account's approval: its refresh token and what it covers.
#[derive(Serialize, Deserialize, Clone)]
struct Grant {
    /// When the refresh token stops working; unknown for a grant without an end.
    expires_at: Option<DateTime<Utc>>,
    /// Token that mints access tokens.
    refresh_token: String,
    /// Granted resources, such as `myactivity.youtube`.
    resources: Vec<String>,
}

impl Grant {
    /// End the grant at Google. A failure is only logged: a new approval still works.
    async fn revoke(&self, app: &App, account: &str) {
        let revoke_url = app.cfg.google_token_url.replace("/token", "/revoke");
        let form = [("token", self.refresh_token.as_str())];
        let revoked = app.http.post(&revoke_url).form(&form).send().await;
        match revoked {
            Ok(reply)
                if reply.status().is_success() || reply.status() == StatusCode::BAD_REQUEST => {}
            Ok(reply) => warn!("google: revoke {account}: {}", reply.status()),
            Err(error) => warn!("google: revoke {account}: {error:#}"),
        }
    }

    /// Activity resources take a time filter; the rest cannot be mixed with them in one export.
    fn split(&self) -> [(&'static str, Vec<String>); 2] {
        let (activity, snapshot) = self
            .resources
            .iter()
            .cloned()
            .partition(|resource| resource.starts_with("myactivity."));
        [("activity", activity), ("snapshot", snapshot)]
    }
}

/// A running export.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
struct Job {
    /// Google's archive job id.
    id: String,
    /// `activity` or `snapshot`.
    kind: String,
    /// Retries so far.
    retries: u32,
}

impl Job {
    /// Download every archive of the finished export and commit the files in them, named by the export's end time.
    async fn commit(
        &self,
        app: &App,
        account: &str,
        until: DateTime<Utc>,
        urls: Option<&Value>,
    ) -> Result<()> {
        let kind = self.kind.as_str();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for url in urls
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            let resp = tri!(app.http.get(url).send().await);
            let status = resp.status();
            if !status.is_success() {
                bail!("download {kind} archive: {status}");
            }
            let archive = tri!(resp.bytes().await);
            for (name, bytes) in tri!(Archive(&archive).unzip()) {
                let inner = name.strip_prefix("Portability/").unwrap_or(&name);
                if inner == INDEX {
                    continue;
                }
                let Ok(text) = String::from_utf8(bytes) else {
                    warn!("google: {account} skipped {name}: not text");
                    continue;
                };
                let (stem, extension) = inner.rsplit_once('.').unwrap_or((inner, "txt"));
                let slug = storage::slug(stem);
                if kind == "snapshot" {
                    let sha_key = key(account, &format!("sha/{slug}"));
                    let Some(sha) = steam::changed(app, &sha_key, &text) else {
                        continue;
                    };
                    hashes.push((sha_key, sha));
                }
                let ext = storage::slug(extension);
                match MAX_PART.split(&text) {
                    None => files.push(File {
                        path: storage::path(SOURCE, account, until, Some(&slug), &ext),
                        content: text,
                    }),
                    Some(parts) => {
                        for (number, part) in (1_usize..).zip(parts) {
                            let suffix = format!("{slug}-part{number}");
                            files.push(File {
                                path: storage::path(SOURCE, account, until, Some(&suffix), &ext),
                                content: part,
                            });
                        }
                    }
                }
            }
        }
        let message = format!("google: {account} {kind} ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (sha_key, sha) in hashes {
            tri!(app.store.set(&sha_key, &sha));
        }
        info!("google: {message}");
        Ok(())
    }

    /// Check the export. Commit its files when it is done and return the job while it is still running.
    async fn poll(mut self, app: &App, account: &str, token: &str) -> Result<Option<Self>> {
        let state_url = format!(
            "{}/v1/archiveJobs/{}/portabilityArchiveState",
            app.cfg.google_portability_url, self.id
        );
        let resp = tri!(app.http.get(&state_url).bearer_auth(token).send().await);
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_success() {
            bail!("GET {state_url}: {status} {}", snippet(&text));
        }
        let state: Value = tri!(serde_json::from_str(&text));
        match state.get("state").and_then(Value::as_str) {
            Some("COMPLETE") => {
                let until: DateTime<Utc> = tri!(tri!(text_field(&state, "exportTime")).parse());
                tri!(self.commit(app, account, until, state.get("urls")).await);
                if self.kind == "activity" {
                    tri!(app.store.set(&key(account, "activity_until"), &until));
                }
                Ok(None)
            }
            Some("FAILED") if self.retries < RETRIES => {
                let retry_url = format!(
                    "{}/v1/archiveJobs/{}:retry",
                    app.cfg.google_portability_url, self.id
                );
                let request = app
                    .http
                    .post(&retry_url)
                    .bearer_auth(token)
                    .json(&json!({}));
                let retry_resp = tri!(request.send().await);
                let retry_status = retry_resp.status();
                let retry_text = tri!(retry_resp.text().await);
                if !retry_status.is_success() {
                    bail!(
                        "retry {} export {}: {retry_status} {}",
                        self.kind,
                        self.id,
                        snippet(&retry_text)
                    );
                }
                warn!(
                    "google: {account} {} export {} failed, retrying",
                    self.kind, self.id
                );
                self.id = tri!(job_id(&retry_text));
                self.retries = self.retries.saturating_add(1);
                Ok(Some(self))
            }
            Some("FAILED" | "CANCELLED") => {
                bail!("{} export {} ended: {state}", self.kind, self.id)
            }
            _ => Ok(Some(self)),
        }
    }
}

/// A size limit on one committed file, in bytes.
struct Limit(usize);

impl Limit {
    /// The parts of a JSON array over the limit, each a JSON array of whole elements under it;
    /// None when the text is small enough or not an array.
    fn split(&self, text: &str) -> Option<Vec<String>> {
        if text.len() <= self.0 {
            return None;
        }
        let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(text) else {
            return None;
        };
        let mut parts = Vec::new();
        let mut part = String::from("[");
        for item in items {
            let grown = part
                .len()
                .saturating_add(item.get().len())
                .saturating_add(2);
            if part.len() > 1 && grown > self.0 {
                part.push(']');
                parts.push(mem::replace(&mut part, String::from("[")));
            }
            if part.len() > 1 {
                part.push(',');
            }
            part.push_str(item.get());
        }
        part.push(']');
        parts.push(part);
        Some(parts)
    }
}

/// The exportable products, each with its Data Portability resources.
struct Products([(&'static str, &'static [&'static str]); 3]);

impl Products {
    /// The scopes of comma-separated products, all of them by default. A brand account has no Maps: use youtube,search.
    fn scopes(&self, products: Option<&String>) -> Result<String> {
        let wanted: Vec<&str> = products.map_or_else(
            || self.0.iter().map(|&(product, _)| product).collect(),
            |list| list.split(',').map(str::trim).collect(),
        );
        let mut out = Vec::new();
        for product in wanted {
            let &(_, resources) = tri!(
                self.0
                    .iter()
                    .find(|&&(name, _)| name == product)
                    .context("unknown product")
            );
            out.extend(
                resources
                    .iter()
                    .map(|resource| format!("{SCOPE_PREFIX}{resource}")),
            );
        }
        Ok(out.join(" "))
    }
}

/// The link to approve `account` again, for the products it granted before.
fn approve_hint(app: &App, account: &str) -> String {
    let resources = app
        .store
        .get::<Grant>(&key(account, "oauth"))
        .map_or_default(|grant| grant.resources);
    let granted: Vec<&str> = PRODUCTS
        .0
        .iter()
        .filter(|&&(_, product_resources)| {
            product_resources
                .iter()
                .any(|resource| resources.iter().any(|known| known == resource))
        })
        .map(|&(product, _)| product)
        .collect();
    let products = if granted.is_empty() {
        String::new()
    } else {
        format!("&products={}", granted.join(","))
    };
    format!(
        "approve again at {}/google/auth?account={account}{products}&key=<ADMIN_SECRET>",
        app.cfg.public_url
    )
}

/// The OAuth client id and secret.
fn client(app: &App) -> Result<(&str, &str)> {
    match (
        app.cfg.google_client_id.as_deref(),
        app.cfg.google_client_secret.as_deref(),
    ) {
        (Some(id), Some(secret)) => Ok((id, secret)),
        _ => bail!("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET are not set"),
    }
}

/// An error reply with the whole error chain as its body.
fn failed<Error>(status: StatusCode, error: &Error) -> Response
where
    Error: Display,
{
    (status, format!("{error:#}")).into_response()
}

/// The `archiveJobId` in Google's answer to starting or retrying an export.
fn job_id(text: &str) -> Result<String> {
    let reply: Value = tri!(serde_json::from_str(text));
    Ok(tri!(text_field(&reply, "archiveJobId")).to_owned())
}

/// The store key of `what` for `account`.
fn key(account: &str, what: &str) -> String {
    format!("google/{account}/{what}")
}

/// The first 300 characters of an error body.
fn snippet(text: &str) -> String {
    text.chars().take(300).collect()
}

/// The string under `name` in a JSON object.
fn text_field<'reply>(reply: &'reply Value, name: &str) -> Result<&'reply str> {
    reply
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| name.to_owned())
}

/// Exchange a code or a refresh token at Google's token endpoint.
async fn token_request(app: &App, params: &[(&str, &str)]) -> Result<Value> {
    let (id, secret) = tri!(client(app));
    let mut form = vec![("client_id", id), ("client_secret", secret)];
    form.extend_from_slice(params);
    let request = app.http.post(&app.cfg.google_token_url).form(&form);
    let resp = tri!(request.send().await);
    let status = resp.status();
    let text = tri!(resp.text().await);
    if !status.is_success() {
        bail!("token request: {status} {}", snippet(&text));
    }
    serde_json::from_str(&text).map_err(Into::into)
}
