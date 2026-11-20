//! KNMI data through the EDR API, which needs a registered key: 10-minute observations from the same stations as
//! `knmi`, and KNMI's daily grids (rain, temperature, evaporation) at home.
//!
//! Daily, each station is asked for its 10-minute data since its last stored time, at most 7 days back, and each grid
//! for the last 60 days at the grid cell around home (the grids are about three weeks behind). Hourly, the grids are
//! backfilled from 1951 in 99-day pieces, 20 per run: KNMI allows at most 100 time steps per request. Replies are
//! `CoverageJSON`, committed verbatim.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::ops::{Add, Sub};
use core::slice;
use core::time::Duration;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, NaiveDate, NaiveTime, SecondsFormat, TimeDelta, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::info;
use url::Url;

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Directory name under `raw/`.
const SOURCE: &str = "knmi_edr";
/// The EDR collection of 10-minute station observations.
const TEN_MINUTE: &str = "10-minute-in-situ-meteorological-observations";
/// Schiphol, `IJmuiden` and Wijk aan Zee, as in `knmi`.
const STATIONS: [&str; 3] = ["0-20000-0-06240", "0-20000-0-06225", "0-20000-0-06257"];
/// How far back a station without a cursor is read, and the longest window asked for at once.
const FIRST_WINDOW: TimeDelta = TimeDelta::days(7);
/// Daily grids: precipitation, mean, minimum and maximum temperature, and Makkink evaporation.
const GRIDS: [&str; 5] = ["Rd1", "Tg1", "Tn1", "Tx1", "EV24"];
/// Days of grids the daily run asks for.
const GRID_RECENT_DAYS: i64 = 60;
/// The first day of the grids.
const GRID_FROM: NaiveDate = NaiveDate::from_ymd_opt(1951, 1, 1).unwrap();
/// The time of day on the grids' time axis.
const GRID_HOUR: NaiveTime = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
/// The first day of the next backfill piece. Removed when done.
const GRID_BACKFILL_KEY: &str = "knmi_edr/grid_backfill_from";
/// Set once the backfill reached the daily window.
const GRID_BACKFILL_DONE: &str = "knmi_edr/grid_backfill_done";
/// Days per request, under KNMI's limit of 100 time steps.
const GRID_CHUNK_DAYS: i64 = 99;
/// Requests per grid per run.
const GRID_CHUNKS_PER_RUN: usize = 20;
/// Attempts per request on a 5xx, waiting 2 s, then 4 s.
const RETRIES: u64 = 3;
/// The wait after the first failed attempt, in milliseconds.
const RETRY_PAUSE_MS: u64 = if cfg!(test) { 0 } else { 2000 };
/// Half the side of the box around home; KNMI's grid cells are about 1 km.
const HALF_BOX: f64 = 0.005;

/// The KNMI EDR source.
pub(super) struct KnmiEdr;

#[async_trait]
impl Source for KnmiEdr {
    async fn daily(&self, app: &App) -> Result<()> {
        if app.cfg.knmi_edr_key.is_none() {
            return Ok(());
        }
        let now = Utc::now();
        let window_start = tri!(now.checked_sub_signed(FIRST_WINDOW).context("window start"));
        let mut files = Vec::new();
        let mut cursors = Vec::new();
        for station in STATIONS {
            let key = format!("knmi_edr/cursor/{station}");
            let since = app
                .store
                .get::<DateTime<Utc>>(&key)
                .unwrap_or(window_start)
                .max(window_start);
            let from = tri!(
                since
                    .checked_add_signed(TimeDelta::seconds(1))
                    .context("second after the cursor")
            );
            let url = tri!(Url::parse_with_params(
                &format!(
                    "{}/collections/{TEN_MINUTE}/locations/{station}",
                    app.cfg.knmi_edr_url
                ),
                [("datetime", format!("{}/{}", iso(from), iso(now)))],
            ));
            let Some(text) = tri!(get(app, &url).await) else {
                continue;
            };
            // The newest time on the time axes of the reply, a coverage collection or a single coverage.
            let reply = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
            let coverages = reply
                .get("coverages")
                .and_then(Value::as_array)
                .map_or_else(|| slice::from_ref(&reply), Vec::as_slice);
            let newest = coverages
                .iter()
                .filter_map(|coverage| coverage.pointer("/domain/axes/t/values"))
                .filter_map(Value::as_array)
                .flatten()
                .filter_map(Value::as_str)
                .filter_map(|time| time.parse::<DateTime<Utc>>().ok())
                .max();
            files.push(tri!(file("ten_minute", station, now, &url, &text)));
            if let Some(time) = newest {
                cursors.push((key, time));
            }
        }
        if let Some(home) = app.cfg.home {
            let start = tri!(
                now.date_naive()
                    .checked_sub_signed(TimeDelta::days(GRID_RECENT_DAYS))
                    .context("grid window start")
            )
            .and_time(GRID_HOUR)
            .and_utc();
            for grid in GRIDS {
                files.extend(tri!(grid_file(app, grid, home, start, now).await));
            }
        }
        drop(tri!(
            app.save(&files, &format!("knmi_edr: daily ({} files)", files.len()))
                .await
        ));
        for (key, time) in cursors {
            tri!(app.store.set(&key, &time));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let (Some(_), Some(home)) = (app.cfg.knmi_edr_key.as_deref(), app.cfg.home) else {
            return Ok(());
        };
        if app.store.get::<bool>(GRID_BACKFILL_DONE) == Some(true) {
            return Ok(());
        }
        // The daily job covers the last GRID_RECENT_DAYS; the backfill stops where it starts.
        let stop = tri!(
            Utc::now()
                .date_naive()
                .checked_sub_signed(TimeDelta::days(GRID_RECENT_DAYS))
                .context("backfill stop")
        );
        let mut day: NaiveDate = app.store.get(GRID_BACKFILL_KEY).unwrap_or(GRID_FROM);
        let mut files = Vec::new();
        for _ in 0..GRID_CHUNKS_PER_RUN {
            if day > stop {
                break;
            }
            let last = tri!(
                day.checked_add_signed(TimeDelta::days(GRID_CHUNK_DAYS - 1))
                    .context("last day of the piece")
            )
            .min(stop);
            for grid in GRIDS {
                let start = day.and_time(GRID_HOUR).and_utc();
                let end = last.and_time(GRID_HOUR).and_utc();
                files.extend(tri!(grid_file(app, grid, home, start, end).await));
            }
            day = tri!(last.succ_opt().context("day after the piece"));
        }
        drop(tri!(
            app.save(&files, &format!("knmi_edr: grids up to {day}"))
                .await
        ));
        if day > stop {
            info!("knmi_edr: grid backfill done");
            tri!(app.store.set(GRID_BACKFILL_DONE, &true));
            tri!(app.store.remove(GRID_BACKFILL_KEY));
        } else {
            tri!(app.store.set(GRID_BACKFILL_KEY, &day));
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

/// Wrap `body` in an envelope, at a path with `suffix` under `kind`.
fn file(kind: &str, suffix: &str, at: DateTime<Utc>, url: &Url, body: &str) -> Result<File> {
    Ok(File {
        path: storage::path(SOURCE, kind, at, Some(suffix), "json"),
        content: tri!(storage::envelope(
            Utc::now(),
            json!({"url": url.as_str()}),
            body
        )),
    })
}

/// GET an EDR URL with the key; None when KNMI has no data in the window (422).
async fn get(app: &App, url: &Url) -> Result<Option<String>> {
    let key = tri!(app.cfg.knmi_edr_key.as_deref().context("KNMI_EDR_KEY"));
    // KNMI answers an occasional 500 to a request that works when repeated.
    let mut attempt = 1_u64;
    let (status, text) = loop {
        let resp = tri!(
            app.http
                .get(url.clone())
                .header("Authorization", key)
                .send()
                .await
        );
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_server_error() || attempt == RETRIES {
            break (status, text);
        }
        sleep(Duration::from_millis(
            RETRY_PAUSE_MS.saturating_mul(attempt),
        ))
        .await;
        attempt = attempt.saturating_add(1);
    };
    if status == StatusCode::UNPROCESSABLE_ENTITY && text.contains("No available datapoints") {
        return Ok(None);
    }
    if !status.is_success() {
        let snippet: String = text.chars().take(300).collect();
        bail!("knmi edr {} {status}: {snippet}", url.path());
    }
    Ok(Some(text))
}

/// One grid's values in the box around `home` from `start` to `end`.
async fn grid_file(
    app: &App,
    grid: &str,
    (lat, lon): (f64, f64),
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Option<File>> {
    let bbox = format!(
        "{:.4},{:.4},{:.4},{:.4}",
        Sub::sub(lon, HALF_BOX),
        Sub::sub(lat, HALF_BOX),
        Add::add(lon, HALF_BOX),
        Add::add(lat, HALF_BOX)
    );
    let url = tri!(Url::parse_with_params(
        &format!("{}/collections/{grid}/cube", app.cfg.knmi_edr_url),
        [
            ("bbox", bbox),
            ("datetime", format!("{}/{}", iso(start), iso(end))),
        ],
    ));
    let Some(text) = tri!(get(app, &url).await) else {
        return Ok(None);
    };
    Ok(Some(tri!(file("grid", grid, start, &url, &text))))
}

/// Format a time as RFC 3339 in whole seconds with a `Z`, as EDR wants.
fn iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}
