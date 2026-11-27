//! Check the query server's status against a fake one.

use anyhow::{Result, ensure};
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use chrono::{Duration, Utc};
use serde_json::{Value, json};

use super::QueryServer;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// Run the check against a server that answers `/s3cret/status` with `status` and `body`.
async fn hourly(status: StatusCode, body: Value) -> Result<()> {
    let base = testing::serve(Router::new().route(
        "/s3cret/status",
        get(async move || (status, body.to_string())),
    ))
    .await;
    let (github, _recorded) = testing::fake_github().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.query_status_url = Some(format!("{base}/s3cret/status"));
    });
    QueryServer.hourly(&app).await
}

/// The error of a failed check, or a note that it passed.
async fn error(status: StatusCode, body: Value) -> String {
    hourly(status, body)
        .await
        .map_or_else(|error| format!("{error:#}"), |()| "passed".to_owned())
}

#[tokio::test]
async fn a_readable_catalog_and_a_recent_build_pass() -> Result<()> {
    let recent = (Utc::now() - Duration::minutes(20)).to_rfc3339();
    tri!(
        hourly(
            StatusCode::OK,
            json!({"catalog": "ok", "last_success": recent, "building_seconds": 120_i64, "last_exit": 0_i32}),
        )
        .await
    );
    Ok(())
}

#[tokio::test]
async fn a_locked_catalog_fails() -> Result<()> {
    let recent = Utc::now().to_rfc3339();
    let got = error(
        StatusCode::OK,
        json!({"catalog": "database is locked", "last_success": recent, "building_seconds": null}),
    )
    .await;
    ensure!(
        got == "queries cannot read the catalog: database is locked",
        "{got}"
    );
    Ok(())
}

#[tokio::test]
async fn a_stale_or_missing_build_fails() -> Result<()> {
    let old = (Utc::now() - Duration::hours(5)).to_rfc3339();
    let got = error(
        StatusCode::OK,
        json!({"catalog": "ok", "last_success": old, "building_seconds": null}),
    )
    .await;
    ensure!(got.starts_with("no build succeeded for 5 hours"), "{got}");
    let never = error(
        StatusCode::OK,
        json!({"catalog": "ok", "last_success": null, "building_seconds": null}),
    )
    .await;
    ensure!(never == "no build succeeded yet", "{never}");
    Ok(())
}

#[tokio::test]
async fn a_long_build_fails() -> Result<()> {
    let recent = Utc::now().to_rfc3339();
    let got = error(
        StatusCode::OK,
        json!({"catalog": "ok", "last_success": recent, "building_seconds": 2400_i64}),
    )
    .await;
    ensure!(got == "a build has run for 40 minutes", "{got}");
    Ok(())
}

#[tokio::test]
async fn errors_hide_the_token() -> Result<()> {
    let got = error(StatusCode::SERVICE_UNAVAILABLE, json!({})).await;
    ensure!(got == "status answered 503 Service Unavailable", "{got}");
    let (github, _recorded) = testing::fake_github().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.query_status_url = Some("http://127.0.0.1:9/s3cret/status".to_owned());
    });
    let unreachable = QueryServer
        .hourly(&app)
        .await
        .map_or_else(|error| format!("{error:#}"), |()| "passed".to_owned());
    ensure!(!unreachable.contains("s3cret"), "{unreachable}");
    Ok(())
}
