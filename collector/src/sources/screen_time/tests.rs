//! Tests for the Mac push endpoint.

use std::path::Path;
use std::sync::PoisonError;

use anyhow::{Context as _, Result, ensure};
use serde_json::Value;

use super::ScreenTime;
use crate::sources::Source as _;
use crate::testing::{self, SCREEN};
use crate::tri;

/// Serve the source with `secret` configured. Returns the base URL and the fake GitHub's record.
async fn served(secret: Option<&str>) -> (String, testing::Shared) {
    let (github, recorded) = testing::fake_github().await;
    let configured = secret.map(String::from);
    let app = testing::app_with(&github, |cfg| cfg.screen_time_secret = configured);
    let base = testing::serve(ScreenTime.router().with_state(app)).await;
    (base, recorded)
}

/// POST `body` to the ingest route, with `token` as bearer when given.
async fn push(base: &str, token: Option<&str>, body: &str) -> Result<reqwest::Response> {
    let mut request = reqwest::Client::new()
        .post(format!("{base}/screen-time/ingest"))
        .body(body.to_owned());
    if let Some(bearer) = token {
        request = request.bearer_auth(bearer);
    }
    let response = tri!(request.send().await);
    Ok(response)
}

/// A missing or wrong token answers 401, no configured secret 503, and nothing is stored.
#[tokio::test]
async fn rejects_a_push_without_the_secret() -> Result<()> {
    let (base, recorded) = served(Some(SCREEN)).await;
    ensure!(
        tri!(push(&base, None, "{}").await).status() == 401,
        "no token"
    );
    ensure!(
        tri!(push(&base, Some("wrong"), "{}").await).status() == 401,
        "wrong token"
    );
    let (unconfigured, _) = served(None).await;
    ensure!(
        tri!(push(&unconfigured, Some(SCREEN), "{}").await).status() == 503,
        "no secret configured"
    );
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

/// A JSON body is stored byte for byte under `raw/screen_time/push/`.
#[tokio::test]
async fn stores_the_body_verbatim() -> Result<()> {
    let (base, recorded) = served(Some(SCREEN)).await;
    let body = r#"{"host": "Mac", "focus": [ ], "claude": [{"type": "user"}]}"#;
    let response = tri!(push(&base, Some(SCREEN), body).await);
    ensure!(response.status() == 200, "accepted");
    let reply: Value = tri!(response.json().await);
    let path = tri!(
        reply
            .get("path")
            .and_then(Value::as_str)
            .context("path in the reply")
    );
    ensure!(
        path.starts_with("raw/screen_time/push/")
            && Path::new(path)
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
    Ok(())
}

/// A body that is not JSON answers 400 and is not stored.
#[tokio::test]
async fn rejects_non_json() -> Result<()> {
    let (base, recorded) = served(Some(SCREEN)).await;
    ensure!(
        tri!(push(&base, Some(SCREEN), "not json").await).status() == 400,
        "refused"
    );
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
