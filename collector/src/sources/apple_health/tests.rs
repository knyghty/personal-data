//! Tests for the Health Auto Export push endpoint.

use alloc::sync::Arc;
use std::path::Path;
use std::sync::PoisonError;

use anyhow::{Context as _, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::{AppleHealth, LAST_RECEIVED, MAIN};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing::{self, APPLE};
use crate::tri;

/// Serve the source. Returns the base URL, the app and the fake GitHub's record.
async fn served() -> (String, Arc<App>, testing::Shared) {
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app(&github);
    let base = testing::serve(AppleHealth.router().with_state(Arc::clone(&app))).await;
    (base, app, recorded)
}

/// The stored path a 200 reply names.
async fn stored_path(response: reqwest::Response) -> Result<String> {
    ensure!(response.status() == 200, "accepted");
    let reply: Value = tri!(response.json().await);
    let path = tri!(
        reply
            .get("path")
            .and_then(Value::as_str)
            .context("path in the reply")
    );
    Ok(path.to_owned())
}

/// A push without the bearer token answers 401 and stores nothing.
#[tokio::test]
async fn rejects_a_push_without_the_secret() -> Result<()> {
    let (base, _app, recorded) = served().await;
    let response = tri!(
        reqwest::Client::new()
            .post(format!("{base}/apple-health/ingest"))
            .body("{}")
            .send()
            .await
    );
    ensure!(response.status() == 401, "refused");
    ensure!(
        recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .blobs
            .is_empty(),
        "nothing stored"
    );
    Ok(())
}

/// A JSON body is stored byte for byte under the slug of the automation name.
#[tokio::test]
async fn stores_the_body_verbatim_under_the_automation_name() -> Result<()> {
    let (base, app, recorded) = served().await;
    let body = r#"{"data": {"metrics": [{"name": "resting_heart_rate", "data": []}]}}"#;
    let response = tri!(
        reqwest::Client::new()
            .post(format!("{base}/apple-health/ingest"))
            .bearer_auth(APPLE)
            .header("automation-name", "Resting Heart Rate")
            .body(body)
            .send()
            .await
    );
    let path = tri!(stored_path(response).await);
    ensure!(
        path.starts_with("raw/apple_health/resting-heart-rate/")
            && Path::new(&path)
                .extension()
                .is_some_and(|extension| extension == "json"),
        "{path}"
    );
    ensure!(
        recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .blobs
            == [body],
        "stored verbatim"
    );
    ensure!(
        app.store.get::<DateTime<Utc>>(LAST_RECEIVED).is_none(),
        "another automation does not count as the main push"
    );
    let main = tri!(
        reqwest::Client::new()
            .post(format!("{base}/apple-health/ingest"))
            .bearer_auth(APPLE)
            .header("automation-name", MAIN)
            .body(body)
            .send()
            .await
    );
    drop(tri!(stored_path(main).await));
    ensure!(
        app.store.get::<DateTime<Utc>>(LAST_RECEIVED).is_some(),
        "the main push is recorded"
    );
    Ok(())
}

/// A push over `PART_BYTES` is stored as parts of the same shape in one commit, every point once and in order.
#[tokio::test]
async fn splits_a_large_push_into_parts() -> Result<()> {
    let (base, _app, recorded) = served().await;
    let points = |count: u32| -> Vec<Value> {
        (0..count)
            .map(|second| json!({"date": format!("2026-11-01 00:00:{second:02} +0100"), "qty": second}))
            .collect()
    };
    let body = json!({"data": {"metrics": [
        {"name": "heart_rate", "units": "count/min", "data": points(12)},
        {"name": "step_count", "units": "count", "data": []},
        {"name": "vo2_max", "units": "ml/(kg*min)", "data": points(3)},
    ]}});
    let response = tri!(
        reqwest::Client::new()
            .post(format!("{base}/apple-health/ingest"))
            .bearer_auth(APPLE)
            .header("automation-name", MAIN)
            .body(body.to_string())
            .send()
            .await
    );
    let path = tri!(stored_path(response).await);
    ensure!(path.ends_with("-part01.json"), "{path}");
    let (blobs, commits) = {
        let state = testing::lock(&recorded);
        (state.blobs.clone(), state.commit_messages.len())
    };
    ensure!(
        blobs.len() > 2 && commits == 1,
        "several parts in one commit: {} blobs",
        blobs.len()
    );
    let mut joined: Vec<(Value, Value)> = Vec::new();
    for blob in &blobs {
        let part: Value = tri!(serde_json::from_str(blob));
        let metrics = tri!(
            part.pointer("/data/metrics")
                .and_then(Value::as_array)
                .context("a part has the push's shape")
        );
        for metric in metrics {
            let name = metric.get("name").cloned().unwrap_or_default();
            for point in metric
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                joined.push((name.clone(), point.clone()));
            }
        }
    }
    let expected: Vec<(Value, Value)> = points(12)
        .into_iter()
        .map(|point| (json!("heart_rate"), point))
        .chain(points(3).into_iter().map(|point| (json!("vo2_max"), point)))
        .collect();
    ensure!(joined == expected, "every point once, in order");
    ensure!(
        blobs.iter().any(|blob| blob.contains("step_count")),
        "a metric without points is kept"
    );
    Ok(())
}

/// A body that is not JSON is stored as text under `export`.
#[tokio::test]
async fn stores_non_json_as_text() -> Result<()> {
    let (base, _app, _recorded) = served().await;
    let response = tri!(
        reqwest::Client::new()
            .post(format!("{base}/apple-health/ingest"))
            .bearer_auth(APPLE)
            .body("not json")
            .send()
            .await
    );
    let path = tri!(stored_path(response).await);
    ensure!(
        path.starts_with("raw/apple_health/export/")
            && Path::new(&path)
                .extension()
                .is_some_and(|extension| extension == "txt"),
        "{path}"
    );
    Ok(())
}
