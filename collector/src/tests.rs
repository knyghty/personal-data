//! Tests for the admin endpoints and the run report.

use alloc::sync::Arc;

use anyhow::{Result, ensure};
use chrono::{TimeDelta, Utc};
use serde_json::{Map, Value, json};

use crate::app::{Failure, Report};
use crate::testing::{self, ADMIN};
use crate::tri;

/// Results per source, as a run report holds them.
fn results<const N: usize>(entries: [(&str, &str); N]) -> Map<String, Value> {
    entries
        .into_iter()
        .map(|(source, result)| (source.to_owned(), json!(result)))
        .collect()
}

/// Failures are listed hourly first, with the `error: ` prefix stripped.
#[test]
fn lists_failures() -> Result<()> {
    let report = Report {
        daily: results([("home_assistant", "error: 502 from HA")]),
        hourly: results([("withings", "ok"), ("weather", "error: no location")]),
        seconds: 0.0_f64,
    };
    let got = report.failures();
    ensure!(
        got == vec![
            Failure {
                error: "no location".into(),
                job: "hourly".into(),
                source: "weather".into(),
            },
            Failure {
                error: "502 from HA".into(),
                job: "daily".into(),
                source: "home_assistant".into(),
            },
        ],
        "the two failures in order"
    );
    Ok(())
}

/// `/health` is open; the admin endpoints answer 401 without the right bearer token.
#[tokio::test]
async fn admin_endpoints_need_the_secret() -> Result<()> {
    let (gh, _) = testing::fake_github().await;
    let base = testing::serve(testing::app(&gh).router()).await;
    let http = reqwest::Client::new();
    let health = tri!(http.get(format!("{base}/health")).send().await);
    ensure!(tri!(health.text().await) == "ok", "health answers ok");
    let run = tri!(http.post(format!("{base}/admin/run")).send().await);
    ensure!(run.status() == 401, "run without a token is unauthorized");
    let wrong = tri!(
        http.get(format!("{base}/admin/state"))
            .bearer_auth("wrong")
            .send()
            .await
    );
    ensure!(
        wrong.status() == 401,
        "state with a wrong token is unauthorized"
    );
    let state = tri!(
        http.get(format!("{base}/admin/state"))
            .bearer_auth(ADMIN)
            .send()
            .await
    );
    ensure!(state.status() == 200, "state with the admin token is ok");
    Ok(())
}

/// With no source configured every hourly job is ok and the fixer is not started.
#[tokio::test]
async fn run_is_ok_when_nothing_is_configured() -> Result<()> {
    let (gh, recorded) = testing::fake_github().await;
    let base = testing::serve(testing::app(&gh).router()).await;
    let resp = tri!(
        reqwest::Client::new()
            .post(format!("{base}/admin/run"))
            .bearer_auth(ADMIN)
            .send()
            .await
    );
    ensure!(resp.status() == 200, "the run is ok");
    let report: Value = tri!(resp.json().await);
    ensure!(
        report
            .get("hourly")
            .and_then(Value::as_object)
            .is_some_and(|hourly| hourly.values().all(|result| result == "ok")),
        "every hourly job is ok"
    );
    ensure!(
        testing::lock(&recorded).fires.is_empty(),
        "the fixer is not started"
    );
    Ok(())
}

/// The first failure only logs, the second reports it and starts the fixer, later ones only report; a success
/// forgets the count.
#[tokio::test]
async fn a_repeated_failure_starts_the_fixer_once_and_every_failed_run_returns_500() -> Result<()> {
    let (gh, recorded) = testing::fake_github().await;
    let app = testing::app(&gh);
    let stale = Utc::now()
        .checked_sub_signed(TimeDelta::days(3))
        .unwrap_or_default();
    tri!(app.store.set("apple_health/last_received", &stale));
    let store = &app.store;
    let base = testing::serve(Arc::clone(&app).router()).await;
    let http = reqwest::Client::new();
    for run in 1_u32..=3 {
        let resp = tri!(
            http.post(format!("{base}/admin/run"))
                .bearer_auth(ADMIN)
                .send()
                .await
        );
        ensure!(resp.status() == 500, "run {run} fails");
        let report: Value = tri!(resp.json().await);
        let err = report
            .pointer("/hourly/apple_health")
            .and_then(Value::as_str)
            .unwrap_or_default();
        ensure!(
            err.starts_with("error: no push from the phone's personal-data-sync-health automation"),
            "run {run}: {err}"
        );
        let fires = testing::lock(&recorded).fires.len();
        ensure!(
            fires == usize::from(run >= 2),
            "run {run}: the first failure only logs, the second starts the fixer, the third does not again"
        );
        ensure!(
            store.get::<u32>("failures/apple_health/hourly") == Some(run),
            "run {run}: the failure count"
        );
    }
    // A success forgets the count.
    tri!(store.set("apple_health/last_received", &Utc::now()));
    let resp = tri!(
        http.post(format!("{base}/admin/run"))
            .bearer_auth(ADMIN)
            .send()
            .await
    );
    ensure!(resp.status() == 200, "the run after the fix is ok");
    ensure!(
        store.get::<u32>("failures/apple_health/hourly").is_none(),
        "a success forgets the count"
    );
    let seen = testing::lock(&recorded);
    ensure!(seen.fires.len() == 1, "the fixer starts once per streak");
    ensure!(
        seen.fires.first().is_some_and(|text| {
            text.starts_with("Collector: apple_health hourly failed\n\nno push from the phone's")
        }),
        "the payload names the source, the job and the error"
    );
    drop(seen);
    Ok(())
}
