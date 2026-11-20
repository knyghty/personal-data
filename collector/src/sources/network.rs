//! Receive pushes from the Home Assistant Pi about the home network and store each payload exactly as received.
//!
//! `pi/push_network.py` runs there every hour and sends the `UniFi` client table, the access point's
//! radio statistics, a summary of the neighbouring networks and the hour's DNS queries from `AdGuard`.

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

use super::Source;
use super::apple_health::push_reply;
use crate::app::App;
use crate::storage::{self, File};

#[cfg(test)]
mod tests;

/// The Pi's pushes, stored one file each.
pub(super) struct Network;

#[async_trait]
impl Source for Network {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "network"
    }

    fn router(&self) -> Router<Arc<App>> {
        let ingest =
            async |State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes| -> Response {
                let Some(secret) = app.cfg.network_secret.as_deref() else {
                    return (StatusCode::SERVICE_UNAVAILABLE, "NETWORK_SECRET is not set")
                        .into_response();
                };
                if App::bearer(&headers) != Some(secret) {
                    warn!(
                        bytes = body.len(),
                        "network: rejected push, wrong or missing bearer token"
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
                    path: storage::path("network", "push", Utc::now(), None, "json"),
                    content: text.to_owned(),
                };
                info!(bytes = body.len(), "network: received push");
                let message = format!("network: push ({} bytes)", body.len());
                let committed = app.save(slice::from_ref(&file), &message).await;
                push_reply("network", &file.path, &committed)
            };
        Router::new().route(
            "/network/ingest",
            post(ingest).layer(DefaultBodyLimit::max(32 << 20)),
        )
    }
}
