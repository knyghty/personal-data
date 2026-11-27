//! Check every hour that the query server can answer queries and keeps its tables fresh.
//!
//! Stores nothing. A failed check fails the run, so two in a row open a Sentry issue and start the fixer.

use alloc::sync::Arc;
use core::time::Duration as Wait;

use anyhow::{Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use super::Source;
use crate::app::App;
use crate::tri;

#[cfg(test)]
mod tests;

/// Fail when no build succeeded for this long; the hourly run ingests files, and a build takes minutes.
const STALE_AFTER: Duration = Duration::hours(3);
/// Fail when one build runs this long; the server kills it after an hour.
const BUILD_LIMIT: Duration = Duration::minutes(30);
/// How long the server may take to answer, a cold start included.
const TIMEOUT: Wait = Wait::from_secs(90);

/// The query server's `/status` answer.
#[derive(Deserialize)]
struct Status {
    /// Seconds the running build has taken, if one runs.
    building_seconds: Option<i64>,
    /// `ok`, or why a query could not read the catalog.
    catalog: String,
    /// When a build last succeeded, if ever since the volume was made.
    last_success: Option<DateTime<Utc>>,
}

/// The query server's health.
pub(super) struct QueryServer;

#[async_trait]
impl Source for QueryServer {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    /// Fail on an unreachable server, an unreadable catalog, a build that runs too long, or no recent good build.
    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(url) = app.cfg.query_status_url.as_deref() else {
            return Ok(());
        };
        // The URL holds the server's token: strip it from errors before they reach logs and Sentry.
        let response = tri!(
            app.http
                .get(url)
                .timeout(TIMEOUT)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
        );
        if !response.status().is_success() {
            bail!("status answered {}", response.status());
        }
        let status: Status = tri!(response.json().await.map_err(reqwest::Error::without_url));
        if status.catalog != "ok" {
            bail!("queries cannot read the catalog: {}", status.catalog);
        }
        let building = Duration::seconds(status.building_seconds.unwrap_or(0));
        if building > BUILD_LIMIT {
            bail!("a build has run for {} minutes", building.num_minutes());
        }
        let Some(at) = status.last_success else {
            bail!("no build succeeded yet");
        };
        let age = Utc::now().signed_duration_since(at);
        if age > STALE_AFTER {
            bail!(
                "no build succeeded for {} hours, last at {at}",
                age.num_hours()
            );
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "query_server"
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}
