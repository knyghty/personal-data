//! Pull the owner's Last.fm scrobbles: every new scrobble hourly, a resumable backfill of the whole history, and daily profile and loved tracks.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::slice;
use core::time::Duration;
use std::collections::HashMap;

use anyhow::{Context as _, Error, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Utc};
use reqwest::{Error as HttpError, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::time::sleep;
use tracing::{info, warn};

use super::luchtmeetnet::RateLimited;
use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// API method for the profile.
const INFO: &str = "user.getinfo";
/// Time of the newest committed scrobble, in Unix seconds.
const LAST_KEY: &str = "lastfm/last";
/// Most results per page the API allows.
const LIMIT: &str = "200";
/// How far before the newest stored scrobble each hourly run looks for scrobbles sent late: Last.fm accepts plays
/// up to 14 days old.
const LOOKBACK_SECS: i64 = 14 * 24 * 3600;
/// API method for the loved tracks.
const LOVED: &str = "user.getlovedtracks";
/// Last.fm asks for no more than about five requests a second.
const PACE_MS: u64 = if cfg!(test) { 0 } else { 250 };
/// Requests per hourly run, so a long history backfills over several runs.
const PAGES_PER_RUN: u64 = if cfg!(test) { 3 } else { 50 };
/// API method for the scrobbles.
const RECENT: &str = "user.getrecenttracks";
/// Extra attempts for 5xx and Last.fm's temporary errors 8, 11 and 16.
const RETRIES: u64 = 2;
/// Pause before the first retry, doubled for the second.
const RETRY_PAUSE_MS: u64 = if cfg!(test) { 0 } else { 2000 };
/// Keys of the stored scrobbles within the lookback, with their times.
const SEEN_KEY: &str = "lastfm/seen";
/// Directory name under `raw/`.
const SOURCE: &str = "lastfm";
/// The backfill window being paged, present while one is unfinished.
const WINDOW_KEY: &str = "lastfm/window";

/// The Last.fm API for the configured user.
struct Api<'app> {
    /// The app, for its HTTP client, configuration and store.
    app: &'app App,
    /// API key, kept out of errors and stored requests.
    key: String,
    /// User whose scrobbles to pull.
    user: String,
}

impl<'app> Api<'app> {
    /// Call `method` for the configured user. Returns the body and the request without the key.
    /// Retries 5xx and temporary errors, and fails with `RateLimited` on error 29 or a 429.
    async fn get(&self, method: &str, extra: &[(&str, String)]) -> Result<(String, Value)> {
        let mut params = vec![("user", self.user.clone())];
        params.extend_from_slice(extra);
        let request = json!({
            "method": method,
            "params": params.iter().map(|param| (param.0.to_owned(), json!(param.1))).collect::<Map<_, _>>(),
        });
        let mut attempt = 0_u64;
        loop {
            sleep(Duration::from_millis(PACE_MS)).await;
            // The key is in the URL, so strip the URL from errors before they reach logs and issues.
            let request_builder = self
                .app
                .http
                .get(&self.app.cfg.lastfm_api_url)
                .query(&[
                    ("method", method),
                    ("api_key", self.key.as_str()),
                    ("format", "json"),
                ])
                .query(&params);
            let resp = tri!(request_builder.send().await.map_err(HttpError::without_url));
            let status = resp.status();
            let text = tri!(resp.text().await.map_err(HttpError::without_url));
            // Errors come as {"error": <code>, "message": ...}, with a 200 or an error status.
            let code = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|reply| reply.get("error").and_then(Value::as_u64));
            if status.is_success() && code.is_none() {
                return Ok((text, request));
            }
            let snippet: String = text.chars().take(300).collect();
            let message = format!("{method} {status}: {snippet}");
            if code == Some(29) || status == StatusCode::TOO_MANY_REQUESTS {
                return Err(Error::msg(RateLimited(message)));
            }
            let temporary = status.is_server_error() || matches!(code, Some(8 | 11 | 16));
            if !temporary || attempt == RETRIES {
                bail!(message);
            }
            attempt = attempt.saturating_add(1);
            warn!("lastfm: {message}, retrying");
            sleep(Duration::from_millis(
                RETRY_PAUSE_MS.saturating_mul(attempt),
            ))
            .await;
        }
    }

    /// The API, when a key and a user are configured.
    fn new(app: &'app App) -> Option<Self> {
        let (Some(key), Some(user)) = (app.cfg.lastfm_api_key.clone(), app.cfg.lastfm_user.clone())
        else {
            return None;
        };
        Some(Self { app, key, user })
    }

    /// Commit the profile and the loved tracks, each only when it changed.
    async fn snapshots(&self) -> Result<()> {
        let app = self.app;
        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        let (info_text, info_request) = tri!(self.get(INFO, &[]).await);
        let info_key = "lastfm/sha/user_info".to_owned();
        if let Some(sha) = steam::changed(app, &info_key, &info_text) {
            files.push(tri!(file(
                "user_info",
                now,
                now,
                None,
                info_request,
                &info_text
            )));
            hashes.push((info_key, sha));
        }
        let mut pages = Vec::new();
        let mut page = 1_u64;
        loop {
            let params = [("limit", LIMIT.to_owned()), ("page", page.to_string())];
            let (text, request) = tri!(self.get(LOVED, &params).await);
            let reply: Value = tri!(serde_json::from_str(&text));
            let total = tri!(
                num(reply.pointer("/lovedtracks/@attr/totalPages"))
                    .context("lovedtracks without totalPages")
            );
            pages.push((text, request));
            if page >= total {
                break;
            }
            page = page.saturating_add(1);
        }
        let loved_key = "lastfm/sha/loved_tracks".to_owned();
        let all: String = pages.iter().map(|entry| entry.0.as_str()).collect();
        if let Some(sha) = steam::changed(app, &loved_key, &all) {
            for (number, (text, request)) in (1_usize..).zip(pages) {
                let suffix = (number > 1).then(|| number.to_string());
                files.push(tri!(file(
                    "loved_tracks",
                    now,
                    now,
                    suffix.as_deref(),
                    request,
                    &text
                )));
            }
            hashes.push((loved_key, sha));
        }
        let message = format!("lastfm: daily ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (sha_key, sha) in hashes {
            tri!(app.store.set(&sha_key, &sha));
        }
        Ok(())
    }
}

/// The source.
pub(super) struct Lastfm;

#[async_trait]
impl Source for Lastfm {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(api) = Api::new(app) else {
            return Ok(());
        };
        let snapshots = api.snapshots().await;
        match snapshots {
            Err(error) if error.is::<RateLimited>() => {
                info!("lastfm: rate limited, skipping the daily snapshots: {error}");
                Ok(())
            }
            other => other,
        }
    }

    /// Backfill while a window is open or nothing is stored yet, otherwise re-read the lookback for new scrobbles.
    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(api) = Api::new(app) else {
            info!("lastfm: not configured, skipping");
            return Ok(());
        };
        let mut pull = Pull {
            files: Vec::new(),
            last: app.store.get(LAST_KEY),
            seen: app.store.get(SEEN_KEY).unwrap_or_default(),
        };
        let open: Option<Window> = app.store.get(WINDOW_KEY);
        let (window, result) = match (open, pull.last) {
            (None, Some(last)) => {
                let rechecked = pull.recheck(&api, last).await;
                rechecked.map_or_else(|error| (None, Err(error)), |window| (window, Ok(())))
            }
            (stored, _) => {
                let mut window = stored.unwrap_or_else(|| Window {
                    page: 1,
                    to: Utc::now().timestamp(),
                });
                let backfilled = window.backfill(&api, &mut pull).await;
                match backfilled {
                    Ok(true) => (None, Ok(())),
                    Ok(false) => (Some(window), Ok(())),
                    Err(error) => (Some(window), Err(error)),
                }
            }
        };
        let message = format!("lastfm: hourly ({} files)", pull.files.len());
        drop(tri!(app.save(&pull.files, &message).await));
        if let Some(last) = pull.last {
            pull.seen
                .retain(|_, time| *time >= last.saturating_sub(LOOKBACK_SECS));
            tri!(app.store.set(LAST_KEY, &last));
        }
        tri!(app.store.set(SEEN_KEY, &pull.seen));
        match window {
            Some(unfinished) => tri!(app.store.set(WINDOW_KEY, &unfinished)),
            None => tri!(app.store.remove(WINDOW_KEY)),
        }
        match result {
            Err(error) if error.is::<RateLimited>() => {
                info!("lastfm: rate limited, resuming next run: {error}");
                Ok(())
            }
            other => other,
        }
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// The state of one hourly pull.
struct Pull {
    /// Files to commit.
    files: Vec<File>,
    /// Time of the newest stored scrobble.
    last: Option<i64>,
    /// Keys of the stored scrobbles within the lookback, with their times.
    seen: HashMap<String, i64>,
}

impl Pull {
    /// Re-read from `LOOKBACK_SECS` before the newest stored scrobble to now and queue the scrobbles not seen before,
    /// because the iPhone scrobbler sends plays in batches with their original times, after the Mac's newer ones.
    /// Moves `last` only when every page was read. Returns a backfill window when too far behind.
    async fn recheck(&mut self, api: &Api<'_>, last: i64) -> Result<Option<Window>> {
        let to = Utc::now().timestamp();
        let from = last.saturating_sub(LOOKBACK_SECS);
        let mut newest = last;
        let mut page = 1_u64;
        loop {
            let params = [
                ("limit", LIMIT.to_owned()),
                ("extended", "1".to_owned()),
                ("from", from.to_string()),
                ("to", to.to_string()),
                ("page", page.to_string()),
            ];
            let (text, mut request) = tri!(api.get(RECENT, &params).await);
            let reply: Value = tri!(serde_json::from_str(&text).context("parse recent tracks"));
            let fresh: Vec<&Value> = tracks(&reply)
                .iter()
                .filter(|track| {
                    uts(track)
                        .is_some_and(|time| self.seen.insert(key(track, time), time).is_none())
                })
                .collect();
            let times: Vec<i64> = fresh.iter().filter_map(|track| uts(track)).collect();
            if let Some(&oldest) = times.iter().min() {
                newest = times.iter().copied().fold(newest, i64::max);
                if let Some(object) = request.as_object_mut() {
                    drop(object.insert("scrobbles".to_owned(), json!(fresh.len())));
                }
                let body = tri!(serde_json::to_string(&fresh));
                let queued = file(
                    "scrobble",
                    tri!(at(oldest)),
                    Utc::now(),
                    None,
                    request,
                    &body,
                );
                self.files.push(tri!(queued));
            }
            let total = tri!(total_pages(&reply));
            if total > PAGES_PER_RUN {
                return Ok(Some(Window { page: 1, to }));
            }
            if page >= total {
                self.last = Some(newest);
                return Ok(None);
            }
            page = page.saturating_add(1);
        }
    }
}

/// The window of an unfinished pull.
#[derive(Serialize, Deserialize)]
struct Window {
    /// Page expected to hold the oldest remaining scrobbles.
    page: u64,
    /// End of the window in Unix seconds.
    to: i64,
}

impl Window {
    /// Queue the scrobbles after the newest stored one, oldest page first, verbatim. True when the window is done.
    ///
    /// The window ends at the time it was opened, and `from` follows the stored scrobbles, so the oldest
    /// remaining scrobbles are always on the last page and a resumed run picks up where the last one stopped.
    async fn backfill(&mut self, api: &Api<'_>, pull: &mut Pull) -> Result<bool> {
        for _ in 0..PAGES_PER_RUN {
            let mut params = vec![
                ("limit", LIMIT.to_owned()),
                ("extended", "1".to_owned()),
                ("to", self.to.to_string()),
                ("page", self.page.to_string()),
            ];
            if let Some(last) = pull.last {
                params.push(("from", last.saturating_add(1).to_string()));
            }
            let (text, request) = tri!(api.get(RECENT, &params).await);
            let reply: Value = tri!(serde_json::from_str(&text).context("parse recent tracks"));
            let total = tri!(total_pages(&reply)).max(1);
            if total != self.page {
                self.page = total;
                continue;
            }
            let mut times = Vec::new();
            for track in tracks(&reply) {
                if let Some(time) = uts(track) {
                    let _: Option<i64> = pull.seen.insert(key(track, time), time);
                    times.push(time);
                }
            }
            if let (Some(&oldest), Some(&newest)) = (times.iter().min(), times.iter().max()) {
                let verbatim = file(
                    "recent_tracks",
                    tri!(at(oldest)),
                    Utc::now(),
                    None,
                    request,
                    &text,
                );
                pull.files.push(tri!(verbatim));
                pull.last = Some(newest);
            }
            if self.page == 1 {
                return Ok(true);
            }
            self.page = self.page.saturating_sub(1);
        }
        Ok(false)
    }
}

/// The time of a scrobble in Unix seconds, as a date.
fn at(uts: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(uts, 0).context("scrobble time out of range")
}

/// A raw file of `kind` at `at`, with the request in its envelope.
fn file(
    kind: &str,
    at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    suffix: Option<&str>,
    request: Value,
    body: &str,
) -> Result<File> {
    Ok(File {
        path: storage::path(SOURCE, kind, at, suffix, "json"),
        content: tri!(storage::envelope(fetched_at, request, body)),
    })
}

/// The key a scrobble is deduplicated on: time, artist and track.
fn key(track: &Value, uts: i64) -> String {
    let artist_name = track
        .get("artist")
        .and_then(|artist| artist.get("name").or_else(|| artist.get("#text")))
        .and_then(Value::as_str);
    format!(
        "{uts} {} {}",
        artist_name.unwrap_or_default(),
        track
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
}

/// A Last.fm number, which comes as a string.
fn num(field: Option<&Value>) -> Option<u64> {
    let Some(value) = field else {
        return None;
    };
    value
        .as_str()
        .and_then(|text| text.parse().ok())
        .or_else(|| value.as_u64())
}

/// The page count of a recent tracks answer.
fn total_pages(reply: &Value) -> Result<u64> {
    num(reply.pointer("/recenttracks/@attr/totalPages")).context("recenttracks without totalPages")
}

/// The tracks of a recent tracks page. A single track comes as an object, not a list.
fn tracks(reply: &Value) -> &[Value] {
    let empty: &[Value] = &[];
    let Some(track) = reply.pointer("/recenttracks/track") else {
        return empty;
    };
    if track.is_object() {
        return slice::from_ref(track);
    }
    track.as_array().map_or(empty, Vec::as_slice)
}

/// The scrobble time of a track. The now playing track has `@attr.nowplaying` and no `date`, so it has none:
/// it is stored as a scrobble once played.
fn uts(track: &Value) -> Option<i64> {
    let Some(seconds) = num(track.pointer("/date/uts")) else {
        return None;
    };
    i64::try_from(seconds).ok()
}
