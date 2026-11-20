//! Look up the `YouTube` videos seen in browser history with the `YouTube` Data API, once per video.
//!
//! The screen time ingest queues the video ids of each push's browser visits; the hourly run looks them up 50 per
//! request. Videos the API does not return (deleted or private) still count as seen, so they are not asked again.
//! To backfill, set `youtube/pending` to a list of ids through `PUT /admin/state`.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use core::str::FromStr;

use anyhow::{Context as _, Error, Result, bail, ensure};
use async_trait::async_trait;
use axum::Router;
use chrono::Utc;
use serde_json::{Value, json};
use tracing::{info, warn};
use url::Url;

use super::{Source, Watches};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Directory name under `raw/`.
const SOURCE: &str = "youtube";
/// Store key of the video ids still to look up.
const PENDING: &str = "youtube/pending";
/// Store key of the video ids looked up, found or not.
const SEEN: &str = "youtube/seen";
/// Ids per `videos.list` request, the API's maximum.
const BATCH: usize = 50;
/// Requests per run; at 1 quota unit each, far below the 10,000 a day.
const BATCHES_PER_RUN: usize = 20;
/// Every part that costs nothing extra; `player` and `localizations` are left out as embed HTML and translations.
const PARTS: &str = "snippet,contentDetails,statistics,status,topicDetails,liveStreamingDetails,recordingDetails,paidProductPlacementDetails";

/// The id of a video, parsed from a watch, short, live, embed or youtu.be URL.
#[derive(Debug, PartialEq, Eq)]
struct VideoId(String);

impl FromStr for VideoId {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self> {
        let parsed = tri!(Url::parse(url));
        let host = tri!(parsed.host_str().context("no host")).to_ascii_lowercase();
        let mut segments = tri!(parsed.path_segments().context("no path"));
        let id = if host == "youtu.be" {
            tri!(segments.next().context("no id")).to_owned()
        } else if host == "youtube.com"
            || host.ends_with(".youtube.com")
            || host.ends_with("youtube-nocookie.com")
        {
            match tri!(segments.next().context("no path")) {
                "watch" => tri!(
                    parsed
                        .query_pairs()
                        .find(|pair| pair.0 == "v")
                        .context("no v parameter")
                )
                .1
                .into_owned(),
                "shorts" | "live" | "embed" | "v" => {
                    tri!(segments.next().context("no id")).to_owned()
                }
                _ => bail!("not a video page"),
            }
        } else {
            bail!("not a YouTube host");
        };
        let valid = id.len() == 11
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
        ensure!(valid, "not a video id: {id}");
        Ok(Self(id))
    }
}

/// The `YouTube` source.
pub(super) struct YouTube;

#[async_trait]
impl Source for YouTube {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(key) = app.cfg.youtube_api_key.as_deref() else {
            return Ok(());
        };
        let pending: Vec<String> = app.store.get(PENDING).unwrap_or_default();
        let todo: Vec<&[String]> = pending.chunks(BATCH).take(BATCHES_PER_RUN).collect();
        if todo.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let mut files = Vec::new();
        for (index, ids) in todo.iter().enumerate() {
            let url = tri!(Url::parse_with_params(
                &format!("{}/videos", app.cfg.youtube_api_url),
                [("part", PARTS), ("id", &ids.join(","))],
            ));
            // The key goes in a header, so it is not in the stored request or in error messages.
            let resp = tri!(
                app.http
                    .get(url.clone())
                    .header("x-goog-api-key", key)
                    .send()
                    .await
            );
            let status = resp.status();
            let text = tri!(resp.text().await);
            if !status.is_success() {
                let snippet: String = text.chars().take(300).collect();
                bail!("youtube videos.list {status}: {snippet}");
            }
            let number = index.saturating_add(1).to_string();
            files.push(File {
                path: storage::path(SOURCE, "video", now, Some(&number), "json"),
                content: tri!(storage::envelope(now, json!({"url": url.as_str()}), &text)),
            });
        }
        let done: Vec<String> = todo.concat();
        drop(tri!(
            app.save(&files, &format!("youtube: {} videos", done.len()))
                .await
        ));
        // Ids queued while the requests ran stay pending: re-read the queue rather than writing back the old copy.
        let mut seen: BTreeSet<String> = app.store.get(SEEN).unwrap_or_default();
        seen.extend(done.iter().cloned());
        tri!(app.store.set(SEEN, &seen));
        let left: Vec<String> = app
            .store
            .get::<Vec<String>>(PENDING)
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !seen.contains(id))
            .collect();
        tri!(app.store.set(PENDING, &left));
        info!(
            "youtube: looked up {} videos, {} left",
            done.len(),
            left.len()
        );
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

impl Watches for YouTube {
    /// Queue the `YouTube` videos in a screen time push's browser visits that were not looked up or queued yet.
    fn queue(&self, app: &App, push: &str) {
        let Ok(visits) = serde_json::from_str::<Value>(push) else {
            return;
        };
        let urls = visits
            .get("browser")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|visit| visit.get("url"))
            .filter_map(Value::as_str);
        let seen: BTreeSet<String> = app.store.get(SEEN).unwrap_or_default();
        let mut pending: Vec<String> = app.store.get(PENDING).unwrap_or_default();
        let before = pending.len();
        for VideoId(id) in urls.filter_map(|url| url.parse::<VideoId>().ok()) {
            if !seen.contains(&id) && !pending.contains(&id) {
                pending.push(id);
            }
        }
        if pending.len() > before
            && let Err(error) = app.store.set(PENDING, &pending)
        {
            warn!("youtube: queue videos: {error:#}");
        }
    }
}
