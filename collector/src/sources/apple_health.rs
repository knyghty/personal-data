//! Receive Health Auto Export pushes and store each payload exactly as received.

use alloc::sync::Arc;
use core::{mem, str};

use anyhow::{Result, bail};
use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use tracing::{info, warn};

use super::Source;
use crate::app::App;
use crate::storage::{self, File};

#[cfg(test)]
mod tests;

/// Store key of the time of the last accepted push from the `MAIN` automation.
const LAST_RECEIVED: &str = "apple_health/last_received";
/// Slug of the automation that pushes the metrics every day; the workout and nutrition ones push only some days.
const MAIN: &str = "personal-data-sync-health";
/// Largest part of a metrics push, in bytes of points; `DuckDB` reads at most 64 MB a file and GitHub 100 MB a blob.
const PART_BYTES: usize = if cfg!(test) { 200 } else { 16 << 20 };
/// Raise an error when the `MAIN` automation has pushed nothing for this long.
const STALE_AFTER: Duration = Duration::hours(24);

/// The phone's Health Auto Export pushes, stored one file each.
pub(super) struct AppleHealth;

#[async_trait]
impl Source for AppleHealth {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    /// Nothing to pull. Fail when `MAIN` pushes stop after they started once, so the run reports it.
    async fn hourly(&self, app: &App) -> Result<()> {
        if let Some(last) = app.store.get::<DateTime<Utc>>(LAST_RECEIVED) {
            let age = Utc::now().signed_duration_since(last);
            if age > STALE_AFTER {
                bail!(
                    "no push from the phone's {MAIN} automation for {} hours, last at {last}",
                    age.num_hours()
                );
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "apple_health"
    }

    fn router(&self) -> Router<Arc<App>> {
        let ingest =
            async |State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes| -> Response {
                let Some(secret) = app.cfg.apple_health_secret.as_deref() else {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "APPLE_HEALTH_SECRET is not set",
                    )
                        .into_response();
                };
                if App::bearer(&headers) != Some(secret) {
                    warn!(
                        automation = header(&headers, "automation-name").unwrap_or("-"),
                        bytes = body.len(),
                        "apple_health: rejected push, wrong or missing bearer token"
                    );
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                let now = Utc::now();
                let kind = header(&headers, "automation-name")
                    .map(storage::slug)
                    .filter(|slug| !slug.is_empty())
                    .unwrap_or_else(|| "export".to_owned());
                let session = header(&headers, "session-id").unwrap_or("-");
                let stored = |suffix: Option<&str>, extension: &str, content: String| File {
                    path: storage::path("apple_health", &kind, now, suffix, extension),
                    content,
                };
                let files = match str::from_utf8(&body) {
                    Ok(text) if serde_json::from_str::<&RawValue>(text).is_ok() => {
                        let split = if text.len() > PART_BYTES
                            && let Ok(push) = serde_json::from_str::<Push<'_>>(text)
                        {
                            push.parts()
                        } else {
                            None
                        };
                        split.map_or_else(
                            || vec![stored(None, "json", text.to_owned())],
                            |parts| {
                                parts
                                    .into_iter()
                                    .zip(1_u32..)
                                    .map(|(content, number)| {
                                        stored(Some(&format!("part{number:02}")), "json", content)
                                    })
                                    .collect()
                            },
                        )
                    }
                    Ok(text) => {
                        warn!("apple_health: body for {kind} is not JSON, storing as text");
                        vec![stored(None, "txt", text.to_owned())]
                    }
                    Err(_) => {
                        warn!("apple_health: body for {kind} is not UTF-8, storing base64");
                        vec![stored(None, "b64", STANDARD.encode(&body))]
                    }
                };
                let bytes = body.len();
                // Free the body before the upload: a backlog push is close to 100 MB.
                drop(body);
                info!(
                    kind,
                    session,
                    bytes,
                    parts = files.len(),
                    automation = header(&headers, "automation-id").unwrap_or("-"),
                    "apple_health: received payload"
                );
                let message = format!("apple_health: {kind} ({bytes} bytes)");
                let committed = app.save(&files, &message).await;
                if committed.is_ok()
                    && kind == MAIN
                    && let Err(error) = app.store.set(LAST_RECEIVED, &now)
                {
                    warn!("apple_health: store last_received: {error:#}");
                }
                let path = files.first().map_or("", |file| file.path.as_str());
                push_reply("apple_health", path, &committed)
            };
        Router::new().route(
            "/apple-health/ingest",
            post(ingest).layer(DefaultBodyLimit::max(512 << 20)),
        )
    }
}

/// A metrics push as Health Auto Export sends it, borrowing every point unparsed.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Push<'text> {
    /// The only top-level key.
    #[serde(borrow)]
    data: Data<'text>,
}

/// The `data` object of a metrics push.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Data<'text> {
    /// One entry per metric.
    #[serde(borrow)]
    metrics: Vec<Metric<'text>>,
}

/// One metric and its points.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Metric<'text> {
    /// The points, each kept as received.
    #[serde(borrow)]
    data: Vec<&'text RawValue>,
    /// The metric name, such as `heart_rate`.
    #[serde(borrow)]
    name: &'text RawValue,
    /// The unit of the points.
    #[serde(borrow)]
    units: &'text RawValue,
}

impl Push<'_> {
    /// Split the push into pushes of the same shape with at most `PART_BYTES` of points each, cutting a metric's points
    /// where a part fills. Returns `None` when a part fails to serialize.
    fn parts(self) -> Option<Vec<String>> {
        let mut parts = Vec::new();
        let (mut part, mut size) = (Vec::new(), 0_usize);
        for metric in self.data.metrics {
            let mut piece = Metric {
                data: Vec::new(),
                ..metric
            };
            for point in metric.data {
                let len = point.get().len().saturating_add(1);
                if size > 0 && size.saturating_add(len) > PART_BYTES {
                    if !piece.data.is_empty() {
                        part.push(Metric {
                            data: mem::take(&mut piece.data),
                            ..piece
                        });
                    }
                    parts.push(mem::take(&mut part));
                    size = 0;
                }
                piece.data.push(point);
                size = size.saturating_add(len);
            }
            part.push(piece);
        }
        parts.push(part);
        parts
            .into_iter()
            .map(|metrics| {
                serde_json::to_string(&Push {
                    data: Data { metrics },
                })
            })
            .collect::<Result<_, _>>()
            .ok()
    }
}

/// The value of header `name`, when it is valid UTF-8.
fn header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let Some(value) = headers.get(name) else {
        return None;
    };
    value.to_str().ok()
}

/// Answer a push: the stored path and commit, or 502 with a warning when the commit failed.
pub(super) fn push_reply(source: &str, path: &str, committed: &Result<Option<String>>) -> Response {
    match committed.as_ref() {
        Ok(sha) => Json(json!({"path": path, "commit": sha})).into_response(),
        Err(error) => {
            warn!("{source}: commit failed: {error:#}");
            (StatusCode::BAD_GATEWAY, format!("commit failed: {error:#}")).into_response()
        }
    }
}
