//! Look up the Twitch channels and past broadcasts in browser history, and store whom the owner follows and subscribes to.
//!
//! The screen time ingest queues the channel logins and video ids of each push's browser visits; the hourly run looks
//! them up 100 per request with an app token. Once the owner approves at `/twitch/auth?key=<ADMIN_SECRET>`, the daily run
//! stores his follows and subscriptions, each only when changed. Twitch has no watch history API: the browser visits
//! are the watch history, and these lookups say what they were.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use core::str::FromStr;
use std::collections::HashMap;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Redirect, Response};
use axum::routing::get;
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};
use tracing::{info, warn};
use url::Url;

use super::{Source, Watches, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Ids per Helix lookup, the API's maximum.
const BATCH: usize = 100;
/// Lookup requests per run; the rest waits for the next run.
const BATCHES_PER_RUN: usize = 20;
/// The lookups, in the order `queue` collects their ids.
const KINDS: [Kind; 2] = [
    Kind {
        name: "user",
        param: "login",
        path: "users",
    },
    Kind {
        name: "video",
        param: "id",
        path: "videos",
    },
];
/// First path segments of twitch.tv that are not channels.
const NOT_CHANNELS: &[&str] = &[
    "directory",
    "videos",
    "settings",
    "subscriptions",
    "inventory",
    "drops",
    "wallet",
    "search",
    "downloads",
    "p",
    "jobs",
    "turbo",
    "prime",
    "friends",
    "messages",
    "payments",
    "popout",
    "moderator",
    "u",
    "login",
    "signup",
    "logout",
    "following",
    "store",
    "broadcast",
    "embed",
    "team",
    "collections",
    "clip",
];
/// Store key of the OAuth state of the approval in progress.
const OAUTH_STATE: &str = "twitch/oauth_state";
/// OAuth scopes the daily run needs.
const SCOPES: &str = "user:read:follows user:read:subscriptions";
/// Directory name under `raw/`.
const SOURCE: &str = "twitch";
/// Store key of the refresh token.
const TOKEN: &str = "twitch/oauth";

/// A Helix client: the app's HTTP client and configuration with an app or user token.
struct Helix<'app> {
    /// The app, for its HTTP client and client id.
    app: &'app App,
    /// The access token.
    token: String,
}

/// A kind of lookup: what is queued, the Helix path and its query key.
struct Kind {
    /// Queue name and file type.
    name: &'static str,
    /// Query key of each id.
    param: &'static str,
    /// Helix path.
    path: &'static str,
}

/// The URL is not a twitch.tv channel or past broadcast.
#[derive(Debug, PartialEq, Eq)]
struct NotTarget;

/// What a twitch.tv URL points at.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// A channel, by login.
    User(String),
    /// A past broadcast, by id.
    Video(String),
}

/// The Twitch source.
pub(super) struct Twitch;

impl Helix<'_> {
    /// GET a Helix URL; a non-2xx reply is an error with the start of its body.
    async fn get(&self, url: &Url) -> Result<String> {
        let resp = tri!(
            self.app
                .http
                .get(url.clone())
                .headers(tri!(self.headers()))
                .send()
                .await
        );
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_success() {
            let snippet: String = text.chars().take(300).collect();
            bail!("twitch {} {status}: {snippet}", url.path());
        }
        Ok(text)
    }

    /// The client id and bearer token headers of a request.
    fn headers(&self) -> Result<HeaderMap> {
        let client_id = tri!(
            self.app
                .cfg
                .twitch_client_id
                .as_deref()
                .context("TWITCH_CLIENT_ID")
        );
        let headers: HeaderMap = [
            (
                HeaderName::from_static("client-id"),
                tri!(HeaderValue::from_str(client_id)),
            ),
            (
                AUTHORIZATION,
                tri!(HeaderValue::from_str(&format!("Bearer {}", self.token))),
            ),
        ]
        .into_iter()
        .collect();
        Ok(headers)
    }

    /// Every item of the paginated list at `path`.
    async fn pages(&self, path: &str, params: &[(&str, &str)]) -> Result<Vec<Value>> {
        let base = format!("{}/{path}", self.app.cfg.twitch_api_url);
        let mut items = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut query = params.to_vec();
            query.push(("first", "100"));
            if let Some(cursor) = after.as_deref() {
                query.push(("after", cursor));
            }
            let url = tri!(Url::parse_with_params(&base, &query));
            let page: Value = tri!(serde_json::from_str(&tri!(self.get(&url).await)));
            items.extend(
                page.get("data")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            let cursor = page
                .pointer("/pagination/cursor")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if cursor.is_empty() {
                return Ok(items);
            }
            after = Some(cursor.to_owned());
        }
    }

    /// The user's subscriptions among the followed broadcasters; Twitch answers 404 for the rest.
    async fn subscriptions(&self, user_id: &str, follows: &[Value]) -> Result<Vec<Value>> {
        let mut subs = Vec::new();
        for follow in follows {
            let Some(broadcaster) = follow.get("broadcaster_id").and_then(Value::as_str) else {
                continue;
            };
            let url = tri!(Url::parse_with_params(
                &format!("{}/subscriptions/user", self.app.cfg.twitch_api_url),
                [("broadcaster_id", broadcaster), ("user_id", user_id)],
            ));
            let resp = tri!(
                self.app
                    .http
                    .get(url)
                    .headers(tri!(self.headers()))
                    .send()
                    .await
            );
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                continue;
            }
            if !status.is_success() {
                bail!("subscription check for {broadcaster}: {status}");
            }
            let body: Value = tri!(resp.json().await);
            subs.extend(
                body.get("data")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        Ok(subs)
    }
}

impl FromStr for Target {
    type Err = NotTarget;

    fn from_str(url: &str) -> Result<Self, NotTarget> {
        let Ok(parsed) = Url::parse(url) else {
            return Err(NotTarget);
        };
        let Some(host) = parsed.host_str() else {
            return Err(NotTarget);
        };
        if !matches!(
            host.to_ascii_lowercase().as_str(),
            "twitch.tv" | "www.twitch.tv" | "m.twitch.tv"
        ) {
            return Err(NotTarget);
        }
        let Some(path) = parsed.path_segments() else {
            return Err(NotTarget);
        };
        let mut segments = path.filter(|segment| !segment.is_empty());
        let Some(first) = segments.next().map(str::to_ascii_lowercase) else {
            return Err(NotTarget);
        };
        if first == "videos" {
            let Some(id) = segments.next() else {
                return Err(NotTarget);
            };
            return if id.bytes().all(|byte| byte.is_ascii_digit()) {
                Ok(Self::Video(id.to_owned()))
            } else {
                Err(NotTarget)
            };
        }
        let valid = (3..=25).contains(&first.len())
            && first
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            && !NOT_CHANNELS.contains(&first.as_str());
        if valid {
            Ok(Self::User(first))
        } else {
            Err(NotTarget)
        }
    }
}

#[async_trait]
impl Source for Twitch {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(mut tokens) = app.store.get::<Value>(TOKEN) else {
            return Ok(());
        };
        let refresh = tri!(
            tokens
                .get("refresh_token")
                .and_then(Value::as_str)
                .context("refresh_token")
        )
        .to_owned();
        let reply = tri!(
            token_request(
                app,
                &[("grant_type", "refresh_token"), ("refresh_token", &refresh)],
            )
            .await
            .context("refresh the Twitch login; approve again at /twitch/auth?key=<ADMIN_SECRET>")
        );
        let stored = tri!(
            tokens
                .as_object_mut()
                .context("twitch/oauth is not an object")
        );
        drop(stored.insert(
            "refresh_token".to_owned(),
            reply.get("refresh_token").cloned().unwrap_or(Value::Null),
        ));
        tri!(app.store.set(TOKEN, &tokens));
        let token = tri!(
            reply
                .get("access_token")
                .and_then(Value::as_str)
                .context("access_token")
        )
        .to_owned();
        let helix = Helix { app, token };

        let me_url = tri!(Url::parse(&format!("{}/users", app.cfg.twitch_api_url)));
        let me: Value = tri!(serde_json::from_str(&tri!(helix.get(&me_url).await)));
        let user = me.pointer("/data/0").cloned().unwrap_or(Value::Null);
        let user_id = tri!(
            user.get("id")
                .and_then(Value::as_str)
                .context("own user id")
        )
        .to_owned();

        let follows = tri!(
            helix
                .pages("channels/followed", &[("user_id", &user_id)])
                .await
        );
        let subs = tri!(helix.subscriptions(&user_id, &follows).await);

        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for (kind, rows) in [("follows", &follows), ("subscriptions", &subs)] {
            let body = json!({"user": user, "data": rows}).to_string();
            let sha_key = format!("twitch/sha/{kind}");
            if let Some(sha) = steam::changed(app, &sha_key, &body) {
                files.push(File {
                    path: storage::path(SOURCE, kind, now, None, "json"),
                    content: tri!(storage::envelope(now, json!({"user_id": user_id}), &body)),
                });
                hashes.push((sha_key, sha));
            }
        }
        drop(tri!(
            app.save(&files, &format!("twitch: daily ({} files)", files.len()))
                .await
        ));
        for (sha_key, sha) in hashes {
            tri!(app.store.set(&sha_key, &sha));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        if app.cfg.twitch_client_id.is_none() {
            return Ok(());
        }
        let queued: usize = KINDS
            .iter()
            .map(|kind| {
                app.store
                    .get::<Vec<String>>(&key(kind.name, "pending"))
                    .map_or(0, |ids| ids.len())
            })
            .sum();
        if queued == 0 {
            return Ok(());
        }
        let reply = tri!(token_request(app, &[("grant_type", "client_credentials")]).await);
        let token = tri!(
            reply
                .get("access_token")
                .and_then(Value::as_str)
                .context("access_token")
        )
        .to_owned();
        let helix = Helix { app, token };
        let now = Utc::now();
        let mut files = Vec::new();
        let mut done = Vec::new();
        for kind in &KINDS {
            let pending: Vec<String> = app
                .store
                .get(&key(kind.name, "pending"))
                .unwrap_or_default();
            let todo: Vec<&[String]> = pending.chunks(BATCH).take(BATCHES_PER_RUN).collect();
            for (number, ids) in (1_usize..).zip(&todo) {
                let params: Vec<(&str, &str)> =
                    ids.iter().map(|id| (kind.param, id.as_str())).collect();
                let url = tri!(Url::parse_with_params(
                    &format!("{}/{}", app.cfg.twitch_api_url, kind.path),
                    params,
                ));
                let text = tri!(helix.get(&url).await);
                let suffix = number.to_string();
                files.push(File {
                    path: storage::path(SOURCE, kind.name, now, Some(&suffix), "json"),
                    content: tri!(storage::envelope(now, json!({"url": url.as_str()}), &text)),
                });
            }
            done.push((kind.name, todo.concat()));
        }
        let count: usize = done.iter().map(|entry| entry.1.len()).sum();
        drop(tri!(
            app.save(&files, &format!("twitch: {count} lookups")).await
        ));
        // Ids queued while the requests ran stay pending: re-read the queue rather than writing back the old copy.
        for (name, ids) in done {
            let mut seen: BTreeSet<String> = app.store.get(&key(name, "seen")).unwrap_or_default();
            seen.extend(ids);
            tri!(app.store.set(&key(name, "seen"), &seen));
            let left: Vec<String> = app
                .store
                .get::<Vec<String>>(&key(name, "pending"))
                .unwrap_or_default()
                .into_iter()
                .filter(|id| !seen.contains(id))
                .collect();
            tri!(app.store.set(&key(name, "pending"), &left));
        }
        info!("twitch: looked up {count} channels and videos");
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        // Send the owner to Twitch to approve the scopes, once the admin secret matches.
        let auth = async |State(app): State<Arc<App>>,
                          Query(query): Query<HashMap<String, String>>|
               -> Response {
            if query.get("key") != Some(&app.cfg.admin_secret) {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let Some(client_id) = app.cfg.twitch_client_id.as_deref() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "TWITCH_CLIENT_ID is not set",
                )
                    .into_response();
            };
            let state = super::withings::random_state();
            if let Err(error) = app.store.set(OAUTH_STATE, &state) {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
            }
            let url = Url::parse_with_params(
                &format!("{}/oauth2/authorize", app.cfg.twitch_id_url),
                [
                    ("response_type", "code"),
                    ("client_id", client_id),
                    (
                        "redirect_uri",
                        &format!("{}/twitch/callback", app.cfg.public_url),
                    ),
                    ("scope", SCOPES),
                    ("state", &state),
                ],
            );
            match url {
                Ok(authorize) => Redirect::temporary(authorize.as_str()).into_response(),
                Err(error) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response()
                }
            }
        };
        // Trade the code Twitch sends back for tokens and store the refresh token.
        let callback = async |State(app): State<Arc<App>>,
                              Query(query): Query<HashMap<String, String>>|
               -> Response {
            if let Some(error) = query.get("error") {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Twitch returned an error: {error}"),
                )
                    .into_response();
            }
            let expected: Option<String> = app.store.get(OAUTH_STATE);
            if expected.is_none() || query.get("state") != expected.as_ref() {
                return (
                    StatusCode::BAD_REQUEST,
                    "state mismatch, start again at /twitch/auth",
                )
                    .into_response();
            }
            let Some(code) = query.get("code") else {
                return (StatusCode::BAD_REQUEST, "missing code").into_response();
            };
            let redirect = format!("{}/twitch/callback", app.cfg.public_url);
            let params = [
                ("grant_type", "authorization_code"),
                ("code", code.as_str()),
                ("redirect_uri", &redirect),
            ];
            let reply = match token_request(&app, &params).await {
                Ok(reply) => reply,
                Err(error) => {
                    return (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response();
                }
            };
            let tokens = json!({"refresh_token": reply.get("refresh_token")});
            if let Err(error) = app.store.set(TOKEN, &tokens) {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
            }
            if let Err(error) = app.store.remove(OAUTH_STATE) {
                warn!("twitch: forget the OAuth state: {error:#}");
            }
            // Run the daily job at the next run rather than in a day.
            if let Err(error) = app.store.remove("last_daily/twitch") {
                warn!("twitch: reset the daily run: {error:#}");
            }
            "Twitch connected. The next run stores your follows and subscriptions.\n"
                .into_response()
        };
        Router::new()
            .route("/twitch/auth", get(auth))
            .route("/twitch/callback", get(callback))
    }
}

impl Watches for Twitch {
    /// Queue the Twitch channels and past broadcasts in a screen time push's browser visits not looked up or queued yet.
    fn queue(&self, app: &App, push: &str) {
        if app.cfg.twitch_client_id.is_none() {
            return;
        }
        let Ok(visits) = serde_json::from_str::<Value>(push) else {
            return;
        };
        let mut logins = Vec::new();
        let mut videos = Vec::new();
        for url in visits
            .get("browser")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|visit| visit.get("url").and_then(Value::as_str))
        {
            match url.parse() {
                Ok(Target::User(login)) => logins.push(login),
                Ok(Target::Video(id)) => videos.push(id),
                Err(NotTarget) => {}
            }
        }
        for (kind, mut ids) in KINDS.iter().zip([logins, videos]) {
            ids.sort_unstable();
            let seen: BTreeSet<String> = app.store.get(&key(kind.name, "seen")).unwrap_or_default();
            let mut pending: Vec<String> = app
                .store
                .get(&key(kind.name, "pending"))
                .unwrap_or_default();
            let before = pending.len();
            for id in ids {
                if !seen.contains(&id) && !pending.contains(&id) {
                    pending.push(id);
                }
            }
            if pending.len() == before {
                continue;
            }
            let stored = app.store.set(&key(kind.name, "pending"), &pending);
            if let Err(error) = stored {
                warn!("twitch: queue {}: {error:#}", kind.name);
            }
        }
    }
}

/// Store key of a lookup queue: `twitch/<kind>/<what>`.
fn key(kind: &str, what: &str) -> String {
    format!("twitch/{kind}/{what}")
}

/// POST the client credentials and `params` to the token endpoint; a non-2xx reply is an error.
async fn token_request(app: &App, params: &[(&str, &str)]) -> Result<Value> {
    let (Some(id), Some(secret)) = (
        app.cfg.twitch_client_id.as_deref(),
        app.cfg.twitch_client_secret.as_deref(),
    ) else {
        bail!("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET are not set");
    };
    let mut form = vec![("client_id", id), ("client_secret", secret)];
    form.extend_from_slice(params);
    let resp = tri!(
        app.http
            .post(format!("{}/oauth2/token", app.cfg.twitch_id_url))
            .form(&form)
            .send()
            .await
    );
    let status = resp.status();
    let text = tri!(resp.text().await);
    if !status.is_success() {
        let snippet: String = text.chars().take(300).collect();
        bail!("twitch token request {status}: {snippet}");
    }
    let reply: Value = tri!(serde_json::from_str(&text));
    Ok(reply)
}
