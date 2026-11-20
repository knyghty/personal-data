//! Tests of the hourly pull, the backfill and the daily station metadata against a fake Luchtmeetnet.

use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse as _, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{Duration, NaiveDate, NaiveTime, Utc};
use serde_json::{Value, json};

use super::{
    BACKFILL_DONE, BACKFILL_FROM, BACKFILL_KEY, BACKFILL_WEEKS, Luchtmeetnet, Source as _,
};
use crate::app::App;
use crate::storage;
use crate::testing;
use crate::tri;

/// Requests the fake Luchtmeetnet saw, the number of measurement pages and whether to redirect ranges.
struct Seen {
    /// Answer 429 once more requests than this arrived.
    limit_after: Option<usize>,
    /// The station's location in its metadata.
    location: &'static str,
    /// Pages of measurements.
    pages: u64,
    /// Redirect requests with a range to the path alone.
    redirect: bool,
    /// The request URIs, in order.
    uris: Vec<String>,
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

/// Record the request and answer one row; a 429 past the limit, a redirect of ranges when configured.
async fn series(State(fake): State<Fake>, uri: Uri) -> Response {
    let mut seen = fake.lock().unwrap_or_else(PoisonError::into_inner);
    seen.uris.push(uri.to_string());
    let limited = seen
        .limit_after
        .is_some_and(|limit| seen.uris.len() > limit);
    let redirect = seen.redirect;
    let pages = seen.pages;
    drop(seen);
    if limited {
        return (StatusCode::TOO_MANY_REQUESTS, "slow down").into_response();
    }
    let query = uri.query().unwrap_or_default();
    if redirect && query.contains("start=") {
        return Redirect::to(uri.path()).into_response();
    }
    let last = if uri.path() == "/measurements" {
        pages
    } else {
        1
    };
    Json(json!({
        "pagination": {"last_page": last},
        "data": [{"station_number": "NL49017", "formula": "NO2", "value": 33.5_f64, "timestamp_measured": "2026-10-16T11:00:00+00:00"}],
    }))
    .into_response()
}

/// An app with one station against the fake.
async fn setup() -> (Arc<App>, testing::Shared, Fake) {
    let (github, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(Seen {
        limit_after: None,
        location: "Amsterdam-Stadhouderskade",
        pages: 1,
        redirect: false,
        uris: vec![],
    }));
    let station = async |State(state): State<Fake>| -> Json<Value> {
        let location = state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .location;
        Json(
            json!({"data": {"type": "Traffic", "location": location, "components": ["NO2", "PM10"]}}),
        )
    };
    let router = Router::new()
        .route("/measurements", get(series))
        .route("/lki", get(series))
        .route("/stations/{id}/", get(station))
        .with_state(Arc::clone(&fake));
    let base = testing::serve(router).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.luchtmeetnet_url = base;
        cfg.luchtmeetnet_stations = vec!["NL49017".into()];
    });
    (app, recorded, fake)
}

/// The hourly run files every page of measurements and the LKI values, with the URL in the envelope.
#[tokio::test]
async fn hourly_commits_every_page_of_measurements_and_lki() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(app.store.set(BACKFILL_DONE, &true));
    fake.lock().unwrap_or_else(PoisonError::into_inner).pages = 2;
    tri!(Luchtmeetnet.hourly(&app).await);
    let uris = fake
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .clone();
    ensure!(uris.len() == 3, "three requests");
    ensure!(
        nth(&uris, 0).starts_with("/measurements?station_number=NL49017&start=20"),
        "first page"
    );
    ensure!(nth(&uris, 0).ends_with("Z&page=1"), "first page number");
    ensure!(nth(&uris, 1).ends_with("&page=2"), "second page");
    ensure!(
        nth(&uris, 2).starts_with("/lki?station_number=NL49017&"),
        "lki"
    );
    let committed = paths(&recorded);
    ensure!(committed.len() == 3, "three files");
    ensure!(
        nth(&committed, 0).starts_with("raw/luchtmeetnet/measurement/")
            && nth(&committed, 0).ends_with("-NL49017.json"),
        "first page path"
    );
    ensure!(
        nth(&committed, 1).ends_with("-NL49017-2.json"),
        "second page path"
    );
    ensure!(
        nth(&committed, 2).starts_with("raw/luchtmeetnet/lki/"),
        "lki path"
    );
    let (messages, first_blob) = {
        let seen = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        (seen.commit_messages.clone(), seen.blobs.first().cloned())
    };
    ensure!(
        messages == ["luchtmeetnet: hourly (3 files)"],
        "commit message"
    );
    let file: Value = tri!(serde_json::from_str(&tri!(first_blob.context("one blob"))));
    ensure!(
        file.pointer("/request/url")
            .and_then(Value::as_str)
            .is_some_and(|url| url.contains("/measurements?")),
        "request url"
    );
    ensure!(
        file.pointer("/response/data/0/formula") == Some(&json!("NO2")),
        "response body"
    );
    Ok(())
}

/// A redirected range is an error and nothing is committed.
#[tokio::test]
async fn a_redirected_range_fails() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    fake.lock().unwrap_or_else(PoisonError::into_inner).redirect = true;
    let Err(error) = Luchtmeetnet.hourly(&app).await else {
        anyhow::bail!("hourly accepted a redirect");
    };
    ensure!(error.to_string().contains("redirected"), "{error}");
    ensure!(paths(&recorded).is_empty(), "no commits");
    Ok(())
}

/// The backfill walks a week per commit from 2019, stops at today and then stays off.
#[tokio::test]
async fn backfill_starts_in_2019_and_walks_weeks_until_today() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Luchtmeetnet.hourly(&app).await);
    let uris = fake
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .clone();
    // Two hourly requests, then two per backfilled week.
    ensure!(uris.len() == 54, "requests: {}", uris.len());
    ensure!(
        nth(&uris, 2).contains("&start=2019-01-01T00%3A00%3A00Z&end=2019-01-08T00%3A00%3A00Z&"),
        "first week"
    );
    ensure!(
        paths(&recorded).contains(&storage::path(
            "luchtmeetnet",
            "measurement",
            BACKFILL_FROM.and_time(NaiveTime::MIN).and_utc(),
            Some("NL49017"),
            "json"
        )),
        "first week path"
    );
    let messages = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit_messages
        .clone();
    // The hourly commit, then one per week.
    ensure!(messages.len() == 27, "commits: {}", messages.len());
    ensure!(
        nth(&messages, 1) == "luchtmeetnet: backfill to 2019-01-08",
        "first week message"
    );
    ensure!(
        nth(&messages, 26) == "luchtmeetnet: backfill to 2019-07-02",
        "last week message"
    );
    let resume = tri!(
        app.store
            .get::<NaiveDate>(BACKFILL_KEY)
            .context("backfill cursor")
    );
    ensure!(
        Some(resume) == BACKFILL_FROM.checked_add_signed(Duration::weeks(26)),
        "cursor after {BACKFILL_WEEKS} weeks"
    );

    // Near today the last window ends now and the backfill stops for good.
    let today = Utc::now().date_naive();
    let recent = tri!(
        today
            .checked_sub_signed(Duration::days(10))
            .context("ten days ago")
    );
    tri!(app.store.set(BACKFILL_KEY, &recent));
    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .clear();
    tri!(Luchtmeetnet.hourly(&app).await);
    let near_today = fake
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .len();
    // Two hourly requests and two weeks of two.
    ensure!(near_today == 6, "requests near today: {near_today}");
    ensure!(
        app.store.get::<NaiveDate>(BACKFILL_KEY).is_none(),
        "cursor removed"
    );
    ensure!(
        app.store.get::<bool>(BACKFILL_DONE) == Some(true),
        "backfill done"
    );
    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .clear();
    tri!(Luchtmeetnet.hourly(&app).await);
    let after_done = fake
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .uris
        .len();
    ensure!(after_done == 2, "only the hourly requests: {after_done}");
    Ok(())
}

/// A 429 keeps the weeks committed so far and their cursor, and the backfill is not marked done.
#[tokio::test]
async fn backfill_keeps_finished_weeks_when_rate_limited() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .limit_after = Some(6);
    tri!(Luchtmeetnet.hourly(&app).await);
    // Hourly took 2 requests, then 2 weeks (4 requests) fit before the limit.
    ensure!(
        app.store.get::<NaiveDate>(BACKFILL_KEY)
            == BACKFILL_FROM.checked_add_signed(Duration::weeks(2)),
        "cursor after two weeks"
    );
    let last_message = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit_messages
        .last()
        .cloned();
    ensure!(
        last_message.as_deref() == Some("luchtmeetnet: backfill to 2019-01-15"),
        "last commit message"
    );
    ensure!(
        app.store.get::<bool>(BACKFILL_DONE) != Some(true),
        "backfill not done"
    );
    Ok(())
}

/// Station metadata is committed on the first run and when it changes.
#[tokio::test]
async fn daily_commits_station_metadata_when_changed() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Luchtmeetnet.daily(&app).await);
    tri!(Luchtmeetnet.daily(&app).await);
    let trees = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trees
        .len();
    ensure!(trees == 1, "one tree after two unchanged runs");
    fake.lock().unwrap_or_else(PoisonError::into_inner).location = "Amsterdam-Stadhouderskade 2";
    tri!(Luchtmeetnet.daily(&app).await);
    let committed = paths(&recorded);
    ensure!(committed.len() == 2, "two files");
    ensure!(
        nth(&committed, 1).starts_with("raw/luchtmeetnet/station/")
            && nth(&committed, 1).ends_with("-NL49017.json"),
        "station path"
    );
    Ok(())
}
