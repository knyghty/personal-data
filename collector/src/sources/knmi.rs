//! Measured weather from KNMI stations near home: Schiphol (240) for Amsterdam, `IJmuiden` (225, wind only) and Wijk
//! aan Zee (257) for the coast. Open-Meteo's weather is a model; these are what the stations measured.
//!
//! KNMI's climatology service returns hourly and daily station data as JSON without a key, validated and about two
//! days behind. The daily job stores the last 10 days of both, so late corrections land too; the hourly job backfills
//! from 2019 three months per run. Replies are committed verbatim.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{Datelike as _, Duration, Months, NaiveDate, Utc};
use serde_json::json;
use tracing::info;

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Store key of the flag set once the backfill reaches the recent window.
const BACKFILL_DONE: &str = "knmi/backfill_done";
/// The first month to backfill.
const BACKFILL_FROM: NaiveDate = match NaiveDate::from_ymd_opt(2019, 1, 1) {
    Some(date) => date,
    None => NaiveDate::MIN,
};
/// Store key of the next month to backfill, removed when the backfill reaches the recent window.
const BACKFILL_KEY: &str = "knmi/backfill_from";
/// Months to backfill per hourly run.
const BACKFILL_MONTHS: u32 = 3;
/// Days the daily job stores again, so late corrections land too.
const RECENT_DAYS: i64 = 10;
/// Directory name under `raw/`.
const SOURCE: &str = "knmi";
/// Station codes, as the service's `stns` parameter.
const STATIONS: &str = "240:225:257";

/// The KNMI climatology source.
pub(super) struct Knmi;

#[async_trait]
impl Source for Knmi {
    async fn daily(&self, app: &App) -> Result<()> {
        if app.cfg.knmi_url.is_empty() {
            return Ok(());
        }
        let today = Utc::now().date_naive();
        let start = tri!(
            today
                .checked_sub_signed(Duration::days(RECENT_DAYS))
                .context("recent window")
        );
        let files = tri!(pull(app, start, today).await);
        drop(tri!(app.save(&files, "knmi: recent").await));
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        if app.cfg.knmi_url.is_empty() || app.store.get::<bool>(BACKFILL_DONE) == Some(true) {
            return Ok(());
        }
        let today = Utc::now().date_naive();
        let recent = tri!(
            today
                .checked_sub_signed(Duration::days(RECENT_DAYS))
                .context("recent window")
        );
        let mut month = app.store.get(BACKFILL_KEY).unwrap_or(BACKFILL_FROM);
        for _ in 0..BACKFILL_MONTHS {
            let next = tri!(
                month
                    .checked_add_months(Months::new(1))
                    .context("next month")
            );
            let end = tri!(next.pred_opt().context("month end"));
            if end >= recent {
                info!("knmi: backfill done");
                tri!(app.store.set(BACKFILL_DONE, &true));
                tri!(app.store.remove(BACKFILL_KEY));
                return Ok(());
            }
            let files = tri!(pull(app, month, end).await);
            let label = format!("{}-{:02}", month.year(), month.month());
            drop(tri!(
                app.save(&files, &format!("knmi: backfill {label}")).await
            ));
            month = next;
            tri!(app.store.set(BACKFILL_KEY, &month));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// Hourly and daily data of every station from `start` to `end`, one file each, named by `start`.
async fn pull(app: &App, start: NaiveDate, end: NaiveDate) -> Result<Vec<File>> {
    let mut files = Vec::new();
    for (kind, path, since, until) in [
        (
            "hourly",
            "uurgegevens",
            format!("{}01", start.format("%Y%m%d")),
            format!("{}24", end.format("%Y%m%d")),
        ),
        (
            "daily",
            "daggegevens",
            start.format("%Y%m%d").to_string(),
            end.format("%Y%m%d").to_string(),
        ),
    ] {
        let url = format!("{}/klimatologie/{path}", app.cfg.knmi_url);
        let form = [
            ("start", since.as_str()),
            ("end", until.as_str()),
            ("vars", "ALL"),
            ("stns", STATIONS),
            ("fmt", "json"),
        ];
        let resp = tri!(app.http.post(&url).form(&form).send().await);
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_success() || !text.trim_start().starts_with('[') {
            let snippet: String = text.chars().take(300).collect();
            bail!("knmi {path} {status}: {snippet}");
        }
        let at = tri!(start.and_hms_opt(0, 0, 0).context("midnight")).and_utc();
        files.push(File {
            path: storage::path(SOURCE, kind, at, Some(&STATIONS.replace(':', "-")), "json"),
            content: tri!(storage::envelope(
                Utc::now(),
                json!({"url": url, "form": form}),
                &text
            )),
        });
    }
    Ok(files)
}
