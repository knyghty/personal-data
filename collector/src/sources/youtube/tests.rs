//! Tests for `youtube`.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::Query;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse as _;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use super::{BATCH, BATCHES_PER_RUN, PARTS, PENDING, Source as _, VideoId, Watches as _, YouTube};
use crate::app::App;
use crate::testing;
use crate::tri;

/// The id lists the fake API was asked for.
type Asked = Arc<Mutex<Vec<String>>>;

/// Look up `BATCHES_PER_RUN` batches per run until the queue is empty.
#[tokio::test]
async fn batches_a_backfill_over_runs() -> Result<()> {
    let (app, recorded, asked) = setup().await;
    let count = tri!(
        BATCH
            .checked_mul(BATCHES_PER_RUN)
            .and_then(|full| full.checked_add(3))
            .context("queue length")
    );
    let ids: Vec<String> = (0..count).map(|number| format!("{number:011}")).collect();
    tri!(app.store.set(PENDING, &ids));
    tri!(YouTube.hourly(&app).await);
    let requests = asked.lock().unwrap_or_else(PoisonError::into_inner).len();
    ensure!(requests == BATCHES_PER_RUN, "one request per batch");
    let files = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trees
        .first()
        .map_or(0, Vec::len);
    ensure!(files == BATCHES_PER_RUN, "one file per batch");
    let left: Vec<String> = tri!(app.store.get(PENDING).context("no queue"));
    ensure!(left.len() == 3, "the last three ids wait for the next run");
    tri!(YouTube.hourly(&app).await);
    let more_requests = asked.lock().unwrap_or_else(PoisonError::into_inner).len();
    ensure!(
        more_requests == BATCHES_PER_RUN.saturating_add(1),
        "one request for the rest"
    );
    let rest: Vec<String> = tri!(app.store.get(PENDING).context("no queue"));
    ensure!(rest.is_empty(), "the queue is empty");
    Ok(())
}

/// Find the id in each supported URL shape and none in the rest.
#[test]
fn finds_video_ids() -> Result<()> {
    let id = Some(VideoId("p72QbRjP4ic".to_owned()));
    let video_id = |url: &str| url.parse::<VideoId>().ok();
    ensure!(
        video_id("https://www.youtube.com/watch?v=p72QbRjP4ic&t=5") == id,
        "watch with a time"
    );
    ensure!(
        video_id("https://m.youtube.com/watch?feature=x&v=p72QbRjP4ic") == id,
        "mobile watch with another parameter first"
    );
    ensure!(
        video_id("https://music.youtube.com/watch?v=p72QbRjP4ic") == id,
        "music watch"
    );
    ensure!(
        video_id("https://youtu.be/p72QbRjP4ic?si=abc") == id,
        "short link"
    );
    ensure!(
        video_id("https://www.youtube.com/shorts/p72QbRjP4ic") == id,
        "short"
    );
    ensure!(
        video_id("https://www.youtube.com/live/p72QbRjP4ic") == id,
        "live"
    );
    ensure!(
        video_id("https://www.youtube-nocookie.com/embed/p72QbRjP4ic") == id,
        "embed"
    );
    ensure!(video_id("https://www.youtube.com/").is_none(), "home page");
    ensure!(
        video_id("https://www.youtube.com/watch?v=short").is_none(),
        "id too short"
    );
    ensure!(
        video_id("https://www.youtube.com/@channel").is_none(),
        "channel page"
    );
    ensure!(
        video_id("https://notyoutube.com/watch?v=p72QbRjP4ic").is_none(),
        "other host"
    );
    ensure!(video_id("not a url").is_none(), "not a URL");
    Ok(())
}

/// Queue the pushed videos, look each up once, and skip the ones already seen, found or not.
#[tokio::test]
async fn looks_up_each_queued_video_once() -> Result<()> {
    let (app, recorded, asked) = setup().await;
    YouTube.queue(
        &app,
        &push(&[
            "https://www.youtube.com/watch?v=aaaaaaaaaaa",
            "https://youtu.be/aaaaaaaaaaa",
            "https://www.youtube.com/watch?v=gone_______",
            "https://example.com/",
        ]),
    );
    tri!(YouTube.hourly(&app).await);
    let requested = asked.lock().unwrap_or_else(PoisonError::into_inner).clone();
    ensure!(
        requested == ["aaaaaaaaaaa,gone_______"],
        "one request with each new id once"
    );
    let (commit_messages, first_path, first_blob) = {
        let recorded_now = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        let first_path = recorded_now
            .trees
            .first()
            .and_then(|tree| tree.first())
            .cloned();
        (
            recorded_now.commit_messages.clone(),
            first_path,
            recorded_now.blobs.first().cloned(),
        )
    };
    ensure!(
        commit_messages == ["youtube: 2 videos"],
        "one commit for the found videos"
    );
    let path = tri!(first_path.context("no file committed"));
    ensure!(path.starts_with("raw/youtube/video/"), "{path}");
    let file: Value = tri!(serde_json::from_str(&tri!(
        first_blob.context("no file committed")
    )));
    ensure!(
        file.pointer("/response/items/0/id").and_then(Value::as_str) == Some("aaaaaaaaaaa"),
        "the reply is stored verbatim"
    );
    let url = tri!(
        file.pointer("/request/url")
            .and_then(Value::as_str)
            .context("no request URL stored")
    );
    ensure!(!url.contains("yt-key"), "the key is not stored");

    // Seen videos, found or not, are not queued or asked for again.
    YouTube.queue(
        &app,
        &push(&["https://www.youtube.com/watch?v=gone_______"]),
    );
    tri!(YouTube.hourly(&app).await);
    let requests = asked.lock().unwrap_or_else(PoisonError::into_inner).len();
    ensure!(requests == 1, "no request for a seen video");
    Ok(())
}

/// A screen time push with browser visits to `urls`.
fn push(urls: &[&str]) -> String {
    let visits: Vec<Value> = urls.iter().map(|url| json!({"url": url})).collect();
    json!({"browser": visits}).to_string()
}

/// An app with a fake `videos.list` that answers with every id but `gone_______` and records the ids asked for.
async fn setup() -> (Arc<App>, testing::Shared, Asked) {
    let (github, recorded) = testing::fake_github().await;
    let asked: Asked = Arc::default();
    let log = Arc::clone(&asked);
    let base = testing::serve(Router::new().route(
        "/videos",
        get(
            async move |headers: HeaderMap, Query(params): Query<HashMap<String, String>>| {
                let keyed =
                    headers.get("x-goog-api-key") == Some(&HeaderValue::from_static("yt-key"));
                if !keyed || params.get("part").map(String::as_str) != Some(PARTS) {
                    return StatusCode::BAD_REQUEST.into_response();
                }
                let ids = params.get("id").cloned().unwrap_or_default();
                log.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(ids.clone());
                let items: Vec<Value> = ids
                    .split(',')
                    .filter(|id| *id != "gone_______")
                    .map(|id| json!({"id": id, "snippet": {"title": "t"}}))
                    .collect();
                Json(json!({"kind": "youtube#videoListResponse", "items": items})).into_response()
            },
        ),
    ))
    .await;
    let app = testing::app_with(&github, |cfg| {
        cfg.youtube_api_key = Some("yt-key".to_owned());
        cfg.youtube_api_url = base;
    });
    (app, recorded, asked)
}
