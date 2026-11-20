//! Tests of the twitch.tv URL parser and the queued lookups.

use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::State;
use axum::http::Uri;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use super::{Source as _, Target, Twitch, Watches as _};
use crate::testing;
use crate::tri;

/// Requests the fake Helix saw.
type Asked = Arc<Mutex<Vec<String>>>;

/// Record the request and answer one row, for `/users` and `/videos`.
async fn lookup(State(asked): State<Asked>, uri: Uri) -> Json<Value> {
    asked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(uri.to_string());
    Json(json!({"data": [{"id": "1"}]}))
}

/// Channel and past broadcast URLs parse; other twitch.tv pages and other hosts do not.
#[test]
fn finds_channels_and_videos() -> Result<()> {
    let target = |url: &str| url.parse::<Target>().ok();
    ensure!(
        target("https://www.twitch.tv/northernlion") == Some(Target::User("northernlion".into())),
        "channel"
    );
    ensure!(
        target("https://twitch.tv/Sips_/videos?filter=archives")
            == Some(Target::User("sips_".into())),
        "channel page, lower-cased"
    );
    ensure!(
        target("https://www.twitch.tv/videos/2560195528?t=1h")
            == Some(Target::Video("2560195528".into())),
        "past broadcast"
    );
    ensure!(
        target("https://www.twitch.tv/directory/category/x").is_none(),
        "directory"
    );
    ensure!(target("https://www.twitch.tv/").is_none(), "front page");
    ensure!(
        target("https://www.twitch.tv/videos/abc").is_none(),
        "video id is not a number"
    );
    ensure!(
        target("https://example.com/northernlion").is_none(),
        "other host"
    );
    Ok(())
}

/// Each queued id is looked up once; ids seen before are not queued again.
#[tokio::test]
async fn looks_up_queued_channels_and_videos_once() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let asked: Asked = Arc::default();
    let base = testing::serve(
        Router::new()
            .route(
                "/oauth2/token",
                post(async || Json(json!({"access_token": "at", "refresh_token": "rt2"}))),
            )
            .route("/users", get(lookup))
            .route("/videos", get(lookup))
            .with_state(Arc::clone(&asked)),
    )
    .await;
    let app = testing::app_with(&github, |cfg| {
        cfg.twitch_client_id = Some("cid".into());
        cfg.twitch_client_secret = Some("secret".into());
        cfg.twitch_api_url = base.clone();
        cfg.twitch_id_url = base;
    });
    let push = |urls: &[&str]| {
        let visits: Vec<Value> = urls.iter().map(|url| json!({"url": url})).collect();
        json!({"browser": visits}).to_string()
    };
    Twitch.queue(
        &app,
        &push(&[
            "https://www.twitch.tv/northernlion",
            "https://www.twitch.tv/videos/42",
            "https://www.twitch.tv/northernlion",
        ]),
    );
    tri!(Twitch.hourly(&app).await);
    let asked_urls = asked.lock().unwrap_or_else(PoisonError::into_inner).clone();
    ensure!(
        asked_urls == ["/users?login=northernlion", "/videos?id=42"],
        "one lookup per kind"
    );
    let (messages, first_tree) = {
        let seen = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        (seen.commit_messages.clone(), seen.trees.first().cloned())
    };
    ensure!(messages == ["twitch: 2 lookups"], "commit message");
    let tree = tri!(first_tree.context("one tree"));
    ensure!(
        tree.first()
            .is_some_and(|path| path.starts_with("raw/twitch/user/"))
            && tree
                .get(1)
                .is_some_and(|path| path.starts_with("raw/twitch/video/")),
        "one file per kind"
    );
    Twitch.queue(&app, &push(&["https://www.twitch.tv/northernlion"]));
    tri!(Twitch.hourly(&app).await);
    let asked_count = asked.lock().unwrap_or_else(PoisonError::into_inner).len();
    ensure!(asked_count == 2, "seen channels are not looked up again");
    Ok(())
}
