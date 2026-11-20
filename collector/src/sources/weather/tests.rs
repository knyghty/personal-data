//! Tests of the hourly and daily fetches and the archive backfill against a fake Open-Meteo.

use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::routing::get;
use chrono::{Datelike as _, Duration, Months, NaiveDate, Utc};
use serde_json::{Value, json};

use super::{ARCHIVE_LAG_DAYS, BACKFILL_KEY, BACKFILL_MONTHS, Source as _, Weather};
use crate::app::App;
use crate::storage;
use crate::testing;
use crate::tri;

/// Requests the fake Open-Meteo saw, as `host/path?query`, and whether to fail air quality.
#[derive(Default)]
struct Seen {
    /// Answer 502 to air quality requests.
    fail_air: bool,
    /// The request URIs, in order.
    urls: Vec<String>,
}

/// The fake's state, shared with the test.
type Fake = Arc<Mutex<Seen>>;

/// The entry at `index`, or an empty string.
fn nth(list: &[String], index: usize) -> &str {
    list.get(index).map_or("", String::as_str)
}

/// The paths of every tree committed so far.
fn paths(recorded: &testing::Shared) -> Vec<String> {
    recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trees
        .iter()
        .flatten()
        .cloned()
        .collect()
}

/// An app with a location against the fake.
async fn setup() -> Result<(Arc<App>, testing::Shared, Fake)> {
    let (github, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::default();
    let reply = async |State(state): State<Fake>, uri: Uri| -> (StatusCode, String) {
        let mut seen = state.lock().unwrap_or_else(PoisonError::into_inner);
        seen.urls.push(uri.to_string());
        let fail_air = seen.fail_air;
        drop(seen);
        if fail_air && uri.path().contains("air-quality") {
            return (StatusCode::BAD_GATEWAY, "down".to_owned());
        }
        (StatusCode::OK, r#"{"hourly": {"time": []}}"#.to_owned())
    };
    let router = Router::new()
        .route("/{host}/v1/{endpoint}", get(reply))
        .with_state(Arc::clone(&fake));
    let base = testing::serve(router).await;
    let app = testing::app_with(&github, |cfg| cfg.open_meteo_url = Some(base));
    tri!(app.store.set(
        "location",
        &json!({"latitude": 52.37310_f64, "longitude": 4.89220_f64}),
    ));
    Ok((app, recorded, fake))
}

/// The request URIs the fake saw so far.
fn urls(fake: &Fake) -> Vec<String> {
    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .urls
        .clone()
}

/// Without a location neither run fetches or commits.
#[tokio::test]
async fn skips_without_a_location() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.open_meteo_url = Some("http://127.0.0.1:1".into());
    });
    tri!(Weather.hourly(&app).await);
    tri!(Weather.daily(&app).await);
    ensure!(paths(&recorded).is_empty(), "no commits");
    Ok(())
}

/// The hourly run asks for the forecast and the air quality in UTC and commits both.
#[tokio::test]
async fn hourly_commits_forecast_and_air_quality() -> Result<()> {
    let (app, recorded, fake) = tri!(setup().await);
    tri!(Weather.hourly(&app).await);
    let seen = urls(&fake);
    ensure!(seen.len() == 2, "two requests");
    let forecast = tri!(
        seen.iter()
            .find(|url| url.contains("/v1/forecast"))
            .context("forecast request")
    );
    ensure!(
        forecast.starts_with(
            "/api.open-meteo.com/v1/forecast?latitude=52.3731&longitude=4.8922&current="
        ),
        "forecast url"
    );
    ensure!(
        forecast.contains("&minutely_15=") && forecast.contains("&past_hours=3&"),
        "forecast params"
    );
    ensure!(forecast.ends_with("&timezone=UTC"), "forecast time zone");
    let air = tri!(
        seen.iter()
            .find(|url| url.contains("/v1/air-quality"))
            .context("air quality request")
    );
    ensure!(
        air.starts_with("/air-quality-api.open-meteo.com/"),
        "air quality url"
    );
    ensure!(
        air.contains("birch_pollen") && air.contains("european_aqi"),
        "air quality params"
    );
    let committed = paths(&recorded);
    ensure!(committed.len() == 2, "two files");
    ensure!(
        nth(&committed, 0).starts_with("raw/weather/open_meteo/"),
        "forecast path"
    );
    ensure!(
        nth(&committed, 1).starts_with("raw/weather/open_meteo_air_quality/"),
        "air quality path"
    );
    let (messages, first_blob) = {
        let recorded_guard = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        (
            recorded_guard.commit_messages.clone(),
            recorded_guard.blobs.first().cloned(),
        )
    };
    ensure!(
        messages == ["weather: open_meteo, open_meteo_air_quality"],
        "commit message"
    );
    let file: Value = tri!(serde_json::from_str(&tri!(first_blob.context("one blob"))));
    ensure!(
        file.pointer("/request/location/latitude") == Some(&json!(52.37310_f64)),
        "location in the envelope"
    );
    ensure!(
        file.pointer("/response/hourly/time") == Some(&json!([])),
        "response body"
    );
    Ok(())
}

/// A failing air quality API is reported after the forecast is committed.
#[tokio::test]
async fn failed_air_quality_keeps_the_forecast() -> Result<()> {
    let (app, recorded, fake) = tri!(setup().await);
    fake.lock().unwrap_or_else(PoisonError::into_inner).fail_air = true;
    let Err(error) = Weather.hourly(&app).await else {
        anyhow::bail!("hourly succeeded with air quality down");
    };
    ensure!(
        error.to_string().contains("open_meteo_air_quality 502"),
        "{error}"
    );
    let committed = paths(&recorded);
    ensure!(committed.len() == 1, "one file");
    ensure!(
        nth(&committed, 0).starts_with("raw/weather/open_meteo/"),
        "forecast path"
    );
    Ok(())
}

/// The daily run asks for the daily variables of yesterday, today and tomorrow.
#[tokio::test]
async fn daily_commits_the_daily_forecast() -> Result<()> {
    let (app, recorded, fake) = tri!(setup().await);
    tri!(Weather.daily(&app).await);
    let seen = urls(&fake);
    ensure!(seen.len() == 1, "one request");
    ensure!(
        nth(&seen, 0).contains("&daily=") && nth(&seen, 0).contains("sunrise"),
        "daily params"
    );
    ensure!(
        nth(&seen, 0).contains("&past_days=2&forecast_days=1&timezone=UTC"),
        "daily range"
    );
    let committed = paths(&recorded);
    ensure!(committed.len() == 1, "one file");
    ensure!(
        nth(&committed, 0).starts_with("raw/weather/open_meteo_daily/"),
        "daily path"
    );
    Ok(())
}

/// The backfill walks a month per pair of requests from the stored date and removes it at the archive's end.
#[tokio::test]
async fn backfill_walks_months_and_stops() -> Result<()> {
    let (app, recorded, fake) = tri!(setup().await);
    let end = tri!(
        Utc::now()
            .date_naive()
            .checked_sub_signed(Duration::days(ARCHIVE_LAG_DAYS))
            .context("archive end")
    );
    let from = tri!(
        end.with_day(1)
            .and_then(|first| first.checked_sub_months(Months::new(13)))
            .and_then(|month| month.with_day(15))
            .context("thirteen months ago")
    );
    tri!(app.store.set(BACKFILL_KEY, &from));

    tri!(Weather.hourly(&app).await);
    let archive: Vec<String> = urls(&fake)
        .into_iter()
        .filter(|url| url.contains("start_date"))
        .collect();
    // Two requests per month.
    ensure!(archive.len() == 24, "archive requests: {}", archive.len());
    let month_end = tri!(
        from.with_day(1)
            .and_then(|first| first.checked_add_months(Months::new(1)))
            .and_then(|next| next.pred_opt())
            .context("end of the first month")
    );
    ensure!(
        nth(&archive, 0).starts_with("/archive-api.open-meteo.com/v1/archive?"),
        "archive url"
    );
    ensure!(
        nth(&archive, 0).contains(&format!("start_date={from}&end_date={month_end}&")),
        "first month range"
    );
    ensure!(
        nth(&archive, 1).starts_with("/air-quality-api.open-meteo.com/"),
        "air quality archive url"
    );
    let midnight = tri!(from.and_hms_opt(0, 0, 0).context("midnight")).and_utc();
    ensure!(
        paths(&recorded).contains(&storage::path(
            "weather",
            "open_meteo_archive",
            midnight,
            None,
            "json"
        )),
        "first month path"
    );
    let resume = tri!(
        app.store
            .get::<NaiveDate>(BACKFILL_KEY)
            .context("backfill cursor")
    );
    ensure!(
        Some(resume)
            == from
                .with_day(1)
                .and_then(|first| first.checked_add_months(Months::new(12))),
        "cursor after {BACKFILL_MONTHS} months"
    );

    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .urls
        .clear();
    tri!(Weather.hourly(&app).await);
    let last_day = tri!(end.pred_opt().context("day before the end"));
    ensure!(
        urls(&fake)
            .iter()
            .any(|url| url.contains(&format!("&end_date={last_day}&"))),
        "last month ends at the archive's end"
    );
    ensure!(
        app.store.get::<NaiveDate>(BACKFILL_KEY).is_none(),
        "cursor removed"
    );
    Ok(())
}
