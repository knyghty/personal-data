//! Earthquakes and other seismic events in and around the Netherlands from KNMI's FDSN event service, no key needed:
//! natural quakes, quakes induced by gas extraction, explosions, quarry blasts and sonic booms.
//!
//! The first daily run stores the whole catalogue; later runs store the last 60 days, as KNMI revises recent events.
//! Replies are FDSN text (pipe-separated), committed verbatim.

use alloc::sync::Arc;
use core::slice;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{Duration, Utc};
use reqwest::StatusCode;
use url::Url;

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

#[cfg(test)]
mod tests;

/// The Netherlands with Belgium, the German border region and the southern North Sea.
const AREA: [(&str, &str); 4] = [
    ("minlatitude", "50.5"),
    ("maxlatitude", "54"),
    ("minlongitude", "3"),
    ("maxlongitude", "7.5"),
];
/// Store key of whether the whole catalogue was stored once.
const BACKFILLED: &str = "earthquakes/backfilled";
/// Days fetched again on every run after the first.
const RECENT_DAYS: i64 = 60;
/// Directory under `raw/`.
const SOURCE: &str = "earthquakes";

/// KNMI's seismic event catalogue.
pub(super) struct Earthquakes;

#[async_trait]
impl Source for Earthquakes {
    async fn daily(&self, app: &App) -> Result<()> {
        if app.cfg.fdsn_url.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let full = app.store.get::<bool>(BACKFILLED) != Some(true);
        let start = if full {
            "1900-01-01".to_owned()
        } else {
            let recent = tri!(
                now.checked_sub_signed(Duration::days(RECENT_DAYS))
                    .context("start of the recent window")
            );
            recent.format("%Y-%m-%d").to_string()
        };
        let mut params: Vec<(&str, &str)> = vec![("starttime", start.as_str()), ("format", "text")];
        params.extend(AREA);
        let url = tri!(Url::parse_with_params(
            &format!("{}/fdsnws/event/1/query", app.cfg.fdsn_url),
            params,
        ));
        let response = tri!(app.http.get(url).send().await);
        let status = response.status();
        let text = tri!(response.text().await);
        // 204 means no events in the window.
        if status == StatusCode::NO_CONTENT {
            return Ok(());
        }
        if !status.is_success() || !text.starts_with("#EventID") {
            bail!(
                "knmi fdsn {status}: {}",
                text.chars().take(300).collect::<String>()
            );
        }
        let kind = if full { "catalogue" } else { "recent" };
        let file = File {
            path: storage::path(SOURCE, kind, now, None, "txt"),
            content: text,
        };
        let message = format!("earthquakes: {kind}");
        drop(tri!(app.save(slice::from_ref(&file), &message).await));
        if full {
            tri!(app.store.set(BACKFILLED, &true));
        }
        Ok(())
    }

    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}
