//! Receive pushes from `personal-data-mac` on the Mac and store each payload exactly as received.

use alloc::sync::Arc;
use core::{slice, str};

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::post;
use chrono::Utc;
use serde_json::value::RawValue;
use tracing::{info, warn};

use super::apple_health::push_reply;
use super::twitch::Twitch;
use super::youtube::YouTube;
use super::{Source, Watches as _};
use crate::app::App;
use crate::storage::{self, File};

#[cfg(test)]
mod tests;

/// The Mac's pushes, stored one file each.
pub(super) struct ScreenTime;

#[async_trait]
impl Source for ScreenTime {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "screen_time"
    }

    fn router(&self) -> Router<Arc<App>> {
        let ingest =
            async |State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes| -> Response {
                let Some(secret) = app.cfg.screen_time_secret.as_deref() else {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "SCREEN_TIME_SECRET is not set",
                    )
                        .into_response();
                };
                if App::bearer(&headers) != Some(secret) {
                    warn!(
                        bytes = body.len(),
                        "screen_time: rejected push, wrong or missing bearer token"
                    );
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                let Some(text) = str::from_utf8(&body)
                    .ok()
                    .filter(|text| serde_json::from_str::<&RawValue>(text).is_ok())
                else {
                    return (StatusCode::BAD_REQUEST, "body is not JSON").into_response();
                };
                let file = File {
                    path: storage::path("screen_time", "push", Utc::now(), None, "json"),
                    content: text.to_owned(),
                };
                info!(bytes = body.len(), "screen_time: received push");
                let message = format!("screen_time: push ({} bytes)", body.len());
                let committed = app.save(slice::from_ref(&file), &message).await;
                if committed.is_ok() {
                    YouTube.queue(&app, text);
                    Twitch.queue(&app, text);
                }
                push_reply("screen_time", &file.path, &committed)
            };
        Router::new().route(
            "/screen-time/ingest",
            post(ingest).layer(DefaultBodyLimit::max(256 << 20)),
        )
    }
}
