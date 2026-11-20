//! Run the KNMI jobs against a fake climatology service.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::State;
use axum::routing::post;
use axum::{Form, Router};
use chrono::{Datelike as _, Duration, NaiveDate, Utc};

use super::{BACKFILL_DONE, BACKFILL_KEY, BACKFILL_MONTHS, Knmi, RECENT_DAYS, STATIONS};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// The forms the fake service received.
type Forms = Arc<Mutex<Vec<HashMap<String, String>>>>;

/// Record the form and answer one hourly row.
async fn data(State(forms): State<Forms>, Form(form): Form<HashMap<String, String>>) -> String {
    forms
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(form);
    r#"[{"station_code":240,"date":"2019-01-01T00:00:00.000Z","hour":1,"T":50}]"#.to_owned()
}

/// The forms received so far.
fn received(forms: &Forms) -> Vec<HashMap<String, String>> {
    forms.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

/// An app pointed at the fake service, the fake GitHub's recorder and the forms the service received.
async fn setup() -> (Arc<App>, testing::Shared, Forms) {
    let (github, recorded) = testing::fake_github().await;
    let forms: Forms = Arc::default();
    let base = testing::serve(
        Router::new()
            .route("/klimatologie/uurgegevens", post(data))
            .route("/klimatologie/daggegevens", post(data))
            .with_state(Arc::clone(&forms)),
    )
    .await;
    (
        testing::app_with(&github, |cfg| cfg.knmi_url = base),
        recorded,
        forms,
    )
}

/// Backfill three months per run, then stop at the recent window.
#[tokio::test]
async fn backfills_months_then_stops() -> Result<()> {
    let (app, recorded, forms) = setup().await;
    tri!(Knmi.hourly(&app).await);
    let asked = received(&forms);
    let months = tri!(usize::try_from(BACKFILL_MONTHS));
    ensure!(
        asked.len() == 2_usize.saturating_mul(months),
        "two forms per month"
    );
    let hourly = tri!(asked.first().context("an hourly form"));
    ensure!(
        hourly
            .get("start")
            .is_some_and(|start| start == "2019010101")
            && hourly.get("end").is_some_and(|end| end == "2019013124"),
        "the hourly form covers January by hour"
    );
    let daily = tri!(asked.get(1).context("a daily form"));
    ensure!(
        daily.get("start").is_some_and(|start| start == "20190101")
            && daily.get("end").is_some_and(|end| end == "20190131"),
        "the daily form covers January by day"
    );
    ensure!(
        hourly.get("stns").is_some_and(|stns| stns == STATIONS),
        "all stations"
    );
    let first_message = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit_messages
        .first()
        .cloned();
    ensure!(
        first_message.as_deref() == Some("knmi: backfill 2019-01"),
        "the commit names the month"
    );
    ensure!(
        app.store.get::<NaiveDate>(BACKFILL_KEY) == NaiveDate::from_ymd_opt(2019, 4, 1),
        "the cursor moves three months"
    );

    let near = tri!(
        Utc::now()
            .date_naive()
            .checked_sub_signed(Duration::days(20))
            .context("a date near today")
    );
    let first_of_month = tri!(near.with_day(1).context("the first of the month"));
    tri!(app.store.set(BACKFILL_KEY, &first_of_month));
    tri!(Knmi.hourly(&app).await);
    tri!(Knmi.hourly(&app).await);
    ensure!(
        app.store.get::<bool>(BACKFILL_DONE) == Some(true),
        "the backfill finishes at the recent window"
    );
    let before = received(&forms).len();
    tri!(Knmi.hourly(&app).await);
    let after = received(&forms).len();
    ensure!(after == before, "a finished backfill asks nothing");
    Ok(())
}

/// Store the last ten days of hourly and daily data.
#[tokio::test]
async fn daily_stores_the_last_ten_days() -> Result<()> {
    let (app, recorded, forms) = setup().await;
    tri!(Knmi.daily(&app).await);
    let today = Utc::now().date_naive();
    let start = tri!(
        today
            .checked_sub_signed(Duration::days(RECENT_DAYS))
            .context("start of the window")
    )
    .format("%Y%m%d")
    .to_string();
    let end = today.format("%Y%m%d").to_string();
    let asked = received(&forms);
    let daily = tri!(asked.get(1).context("a daily form"));
    ensure!(daily.get("start") == Some(&start), "ten days back");
    ensure!(daily.get("end") == Some(&end), "up to today");
    let committed = recorded.lock().unwrap_or_else(PoisonError::into_inner);
    let tree = tri!(committed.trees.first().context("a commit"));
    ensure!(
        tree.first()
            .is_some_and(|path| path.starts_with("raw/knmi/hourly/"))
            && tree
                .get(1)
                .is_some_and(|path| path.starts_with("raw/knmi/daily/")),
        "an hourly and a daily file"
    );
    drop(committed);
    Ok(())
}
