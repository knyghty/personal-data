//! Tests for `water`.

use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::http::StatusCode;
use axum::response::IntoResponse as _;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, DurationRound as _, TimeDelta, Utc};
use serde_json::{Value, json};

use super::{LIVE, Source as _, Water, rws_time};
use crate::testing;
use crate::tri;

/// Commit the decoded spot list of the home page, fail without one, and write Rijkswaterstaat times.
#[tokio::test]
async fn finds_the_spot_list_and_writes_rws_times() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let html = r#"<div data-swimming-spots='[{"spotid": "1", "name": "Nieuwe Meer &amp; co", "status" : "goed"}]'></div>"#;
    let base = testing::serve(zwemwater(html)).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.rws_url.clone_from(&base);
        cfg.zwemwater_url = base;
    });
    tri!(Water.daily(&app).await);
    let committed = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .blobs
        .first()
        .cloned();
    let file: Value = tri!(serde_json::from_str(&tri!(
        committed.context("no spot list committed")
    )));
    ensure!(
        file.pointer("/response/0/name").and_then(Value::as_str) == Some("Nieuwe Meer & co"),
        "entities decoded in the committed list"
    );

    let blank = testing::serve(zwemwater("<div></div>")).await;
    let blank_app = testing::app_with(&github, |cfg| {
        cfg.rws_url.clone_from(&blank);
        cfg.zwemwater_url = blank;
    });
    let error = tri!(
        Water
            .daily(&blank_app)
            .await
            .err()
            .context("a page without a spot list was accepted")
    );
    ensure!(
        format!("{error:#}").contains("no swimming spot list"),
        "{error:#}"
    );

    let time = tri!(DateTime::parse_from_rfc3339("2026-07-01T12:00:00Z")).with_timezone(&Utc);
    ensure!(
        rws_time(time) == "2026-07-01T13:00:00.000+01:00",
        "a fixed +01:00 offset"
    );
    Ok(())
}

/// Store the station with data, resume from its newest measurement and commit nothing when nothing is new.
#[tokio::test]
async fn stores_new_measurements_and_resumes_from_the_newest() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    // The newest measurement the fake serves: two hours ago, on a 10-minute mark, so it stays inside `FIRST_WINDOW`.
    let newest = tri!(
        tri!(
            Utc::now()
                .checked_sub_signed(TimeDelta::hours(2))
                .context("two hours ago")
        )
        .duration_trunc(TimeDelta::minutes(10))
    );
    let earlier = tri!(
        newest
            .checked_sub_signed(TimeDelta::minutes(10))
            .context("ten minutes earlier")
    );
    let posted: Arc<Mutex<Vec<Value>>> = Arc::default();
    let log = Arc::clone(&posted);
    let base = testing::serve(
        Router::new()
            .route(
                "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
                post(async move |Json(body): Json<Value>| {
                    let mut bodies = log.lock().unwrap_or_else(PoisonError::into_inner);
                    bodies.push(body.clone());
                    let second_run = bodies.len() > LIVE.len();
                    drop(bodies);
                    let station = body.pointer("/Locatie/Code").and_then(Value::as_str);
                    if station != Some("amsterdam.ndsmwerf") || second_run {
                        return StatusCode::NO_CONTENT.into_response();
                    }
                    Json(json!({"Succesvol": true, "WaarnemingenLijst": [{"MetingenLijst": [
                        {"Tijdstip": rws_time(earlier), "Meetwaarde": {"Waarde_Numeriek": 16.5_f64}},
                        {"Tijdstip": rws_time(newest), "Meetwaarde": {"Waarde_Numeriek": 16.4_f64}},
                    ]}]}))
                    .into_response()
                }),
            )
            .route("/", get(async || "x")),
    )
    .await;
    let app = testing::app_with(&github, |cfg| cfg.rws_url = base);
    tri!(Water.hourly(&app).await);
    let (commit_messages, first_path) = {
        let recorded_now = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        let first_path = recorded_now
            .trees
            .first()
            .and_then(|tree| tree.first())
            .cloned();
        (recorded_now.commit_messages.clone(), first_path)
    };
    ensure!(
        commit_messages == ["water: temperature (1 stations)"],
        "one station had data"
    );
    let path = tri!(first_path.context("no file committed"));
    ensure!(
        path.starts_with("raw/water/temperature/") && path.ends_with("-amsterdam.ndsmwerf.json"),
        "{path}"
    );
    let cursor: DateTime<Utc> = tri!(
        app.store
            .get("water/cursor/amsterdam.ndsmwerf")
            .context("no cursor")
    );
    ensure!(cursor == newest, "the cursor is the newest measurement");

    tri!(Water.hourly(&app).await);
    let second_start = posted
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(LIVE.len())
        .and_then(|body| body.pointer("/Periode/Begindatumtijd"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    ensure!(
        second_start == Some(rws_time(newest)),
        "the second run starts at the cursor"
    );
    let commits = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit_messages
        .len();
    ensure!(commits == 1, "nothing new, nothing committed");
    Ok(())
}

/// A fake zwemwater.nl serving `html` at `/`, with a Rijkswaterstaat without measurements.
fn zwemwater(html: &'static str) -> Router {
    Router::new().route("/", get(async move || html)).route(
        "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        post(async || StatusCode::NO_CONTENT),
    )
}
