//! Tests for `knmi_edr`.

use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use serde_json::json;

use super::{
    GRID_BACKFILL_DONE, GRID_BACKFILL_KEY, GRID_CHUNK_DAYS, GRID_CHUNKS_PER_RUN, GRID_FROM,
    GRID_RECENT_DAYS, GRIDS, KnmiEdr, STATIONS, Source as _, iso,
};
use crate::testing;
use crate::tri;

/// The fake EDR API: the requests it saw.
#[derive(Default)]
struct Edr {
    /// Every request path with its query.
    seen: Vec<String>,
}

impl Edr {
    /// Two ten-minute steps for every station and grid but station `06225` (`IJmuiden`), which has no data.
    fn answer(&mut self, headers: &HeaderMap, uri: &Uri) -> Response {
        if headers.get("authorization") != Some(&HeaderValue::from_static("edr-key")) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        self.seen.push(uri.to_string());
        if uri.path().contains("06225") {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                "No available datapoints in given datetime period",
            )
                .into_response();
        }
        Json(json!({"type": "CoverageCollection", "coverages": [{"domain": {"axes": {"t": {"values": [
            "2026-10-19T18:00:00Z", "2026-10-19T18:10:00Z"]}}}, "ranges": {}}]}))
        .into_response()
    }
}

/// The fake behind the handler.
type Shared = Arc<Mutex<Edr>>;

/// The requests the fake saw so far.
fn seen(edr: &Shared) -> Vec<String> {
    edr.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .seen
        .clone()
}

/// Pull the stations and grids daily, then backfill the grids hourly in pieces up to the daily window.
#[tokio::test]
async fn stores_ten_minute_data_and_grids_and_resumes() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let edr: Shared = Arc::default();
    let fake = Arc::clone(&edr);
    let base = testing::serve(Router::new().fallback(get(
        async move |headers: HeaderMap, uri: Uri| {
            fake.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .answer(&headers, &uri)
        },
    )))
    .await;
    let app = testing::app_with(&github, |cfg| {
        cfg.knmi_edr_key = Some("edr-key".to_owned());
        cfg.knmi_edr_url = base;
        cfg.home = Some((52.37_f64, 4.89_f64));
    });
    tri!(KnmiEdr.daily(&app).await);
    let daily_requests = seen(&edr);
    ensure!(
        daily_requests.len() == STATIONS.len().saturating_add(GRIDS.len()),
        "one request per station and per grid"
    );
    // The three stations, then the grids.
    let rd1 = tri!(
        daily_requests
            .get(STATIONS.len())
            .context("no grid request")
    );
    ensure!(
        rd1.starts_with("/collections/Rd1/cube?bbox=4.8850%2C52.3650%2C4.8950%2C52.3750&datetime="),
        "{rd1}"
    );
    let committed = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trees
        .first()
        .map_or(0, Vec::len);
    ensure!(
        committed == GRIDS.len().saturating_add(2),
        "IJmuiden had no data"
    );
    let cursor: DateTime<Utc> = tri!(
        app.store
            .get("knmi_edr/cursor/0-20000-0-06240")
            .context("no cursor for Schiphol")
    );
    ensure!(
        iso(cursor) == "2026-10-19T18:10:00Z",
        "the cursor is the newest time"
    );
    ensure!(
        app.store
            .get::<DateTime<Utc>>("knmi_edr/cursor/0-20000-0-06225")
            .is_none(),
        "no cursor without data"
    );

    let before = seen(&edr).len();
    tri!(KnmiEdr.hourly(&app).await);
    let days_per_run = tri!(
        GRID_CHUNK_DAYS
            .checked_mul(tri!(i64::try_from(GRID_CHUNKS_PER_RUN)))
            .context("days per run")
    );
    let resume = tri!(
        GRID_FROM
            .checked_add_signed(TimeDelta::days(days_per_run))
            .context("resume day")
    );
    ensure!(
        app.store.get::<NaiveDate>(GRID_BACKFILL_KEY) == Some(resume),
        "the backfill resumes after this run's pieces"
    );
    let hourly_requests = seen(&edr);
    ensure!(
        hourly_requests.len().saturating_sub(before)
            == GRIDS.len().saturating_mul(GRID_CHUNKS_PER_RUN),
        "one request per grid per piece"
    );
    // 99 days per request: 1951-01-01 to 1951-04-09, both at 08:00.
    let first = tri!(hourly_requests.get(before).context("no backfill request"));
    ensure!(
        first.contains("datetime=1951-01-01T08%3A00%3A00Z%2F1951-04-09T08%3A00%3A00Z"),
        "{first}"
    );

    // Near today the backfill stops at the daily window and is done.
    let near_today = tri!(
        Utc::now()
            .date_naive()
            .checked_sub_signed(TimeDelta::days(GRID_RECENT_DAYS + 5))
            .context("day inside the last piece")
    );
    tri!(app.store.set(GRID_BACKFILL_KEY, &near_today));
    tri!(KnmiEdr.hourly(&app).await);
    ensure!(
        app.store.get::<bool>(GRID_BACKFILL_DONE) == Some(true),
        "the backfill is done"
    );
    Ok(())
}
