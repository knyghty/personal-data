//! Tests against a fake Last.fm that serves scrobbles, the profile and loved tracks, and records every query.

use core::mem;
use core::str::FromStr;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::ensure;
use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;

use super::*;
use crate::testing;

/// A scrobble time to count from.
const T0: i64 = 1_700_000_000;

/// The fake behind a lock.
type Fake = Arc<Mutex<Seen>>;

/// What the fake Last.fm serves and saw.
#[derive(Default)]
struct Seen {
    /// Answer error 29 once this many requests were served.
    limit_after: Option<usize>,
    /// Name of the one loved track; empty for none.
    loved: &'static str,
    /// Serve a now playing track on top of the first page.
    now_playing: bool,
    /// Every query received.
    queries: Vec<HashMap<String, String>>,
    /// Scrobble times, any order.
    scrobbles: Vec<i64>,
    /// Requests still to answer with error 16.
    temporary: usize,
}

/// Scrobble times in every committed file, sorted.
fn committed(recorded: &testing::Shared) -> Result<Vec<i64>> {
    let seen = lock(recorded);
    let mut out = Vec::new();
    for blob in &seen.blobs {
        let envelope: Value = tri!(serde_json::from_str(blob));
        // Backfill pages are verbatim; lookback files are lists of new tracks.
        let response = envelope.get("response").unwrap_or(&Value::Null);
        let items = response
            .as_array()
            .map_or_else(|| tracks(response), Vec::as_slice);
        out.extend(items.iter().filter_map(uts));
    }
    drop(seen);
    out.sort_unstable();
    Ok(out)
}

/// Lock a fake, poisoned or not.
fn lock<Inner>(shared: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A numeric query parameter, when present and well-formed.
fn param<Number>(query: &HashMap<String, String>, name: &str) -> Option<Number>
where
    Number: FromStr,
{
    let Some(text) = query.get(name) else {
        return None;
    };
    text.parse().ok()
}

/// Take the queries the fake saw so far.
fn queries(fake: &Fake) -> Vec<HashMap<String, String>> {
    mem::take(&mut lock(fake).queries)
}

/// An app wired to a fake GitHub and a fake Last.fm holding `scrobbles`.
async fn setup(scrobbles: Vec<i64>) -> (Arc<App>, testing::Shared, Fake) {
    let (github, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(Seen {
        scrobbles,
        ..Default::default()
    }));
    let api = async |State(shared): State<Fake>,
                     Query(query): Query<HashMap<String, String>>|
           -> Response {
        assert_eq!(query.get("api_key").map(String::as_str), Some("lastfm-key"));
        assert_eq!(query.get("user").map(String::as_str), Some("alex"));
        assert_eq!(query.get("format").map(String::as_str), Some("json"));
        let mut seen = lock(&shared);
        seen.queries.push(query.clone());
        if seen
            .limit_after
            .is_some_and(|limit| seen.queries.len() > limit)
        {
            let body = json!({"error": 29_u64, "message": "Rate Limit Exceeded"});
            return (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
        }
        if seen.temporary > 0 {
            seen.temporary = seen.temporary.saturating_sub(1);
            let body = json!({"error": 16_u64, "message": "There was a temporary error processing your request."});
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
        }
        let page: usize = param(&query, "page").unwrap_or(1);
        let body = match query.get("method").map(String::as_str) {
            Some(RECENT) => {
                let from: i64 = param(&query, "from").unwrap_or(0);
                let to: i64 = param(&query, "to").unwrap_or(i64::MAX);
                let limit: usize = param(&query, "limit").unwrap_or(50);
                let mut times: Vec<i64> = seen
                    .scrobbles
                    .iter()
                    .copied()
                    .filter(|time| (from..=to).contains(time))
                    .collect();
                times.sort_unstable_by(|left, right| right.cmp(left));
                let total = times.len().div_ceil(limit);
                let mut list: Vec<Value> = times
                    .chunks(limit)
                    .nth(page.saturating_sub(1))
                    .unwrap_or_default()
                    .iter()
                    .map(|&time| track(time))
                    .collect();
                if seen.now_playing && page == 1 {
                    let mut playing = track(0);
                    if let Some(object) = playing.as_object_mut() {
                        drop(object.remove("date"));
                        drop(object.insert("@attr".to_owned(), json!({"nowplaying": "true"})));
                    }
                    list.insert(0, playing);
                }
                let track_value = if list.len() == 1 {
                    list.pop().unwrap_or_default()
                } else {
                    Value::Array(list)
                };
                json!({"recenttracks": {"track": track_value, "@attr": {
                    "user": "alex", "totalPages": total.to_string(), "page": page.to_string(),
                    "perPage": limit.to_string(), "total": times.len().to_string(),
                }}})
            }
            Some(INFO) => {
                json!({"user": {"name": "alex", "playcount": seen.scrobbles.len().to_string(), "registered": {"unixtime": "1037793040", "#text": 1_037_793_040_u64}}})
            }
            Some(LOVED) => {
                let total = if seen.loved.is_empty() { "0" } else { "2" };
                json!({"lovedtracks": {"track": [{"name": seen.loved, "date": {"uts": "1700000000"}}], "@attr": {"user": "alex", "page": page.to_string(), "totalPages": total}}})
            }
            Some(other) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("unexpected method {other}"),
                )
                    .into_response();
            }
            None => return (StatusCode::BAD_REQUEST, "missing method").into_response(),
        };
        drop(seen);
        Json(body).into_response()
    };
    let router = Router::new()
        .route("/2.0/", get(api))
        .with_state(Arc::clone(&fake));
    let base = testing::serve(router).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.lastfm_api_key = Some("lastfm-key".to_owned());
        cfg.lastfm_user = Some("alex".to_owned());
        cfg.lastfm_api_url = format!("{base}/2.0/");
    });
    (app, recorded, fake)
}

/// A track scrobbled at `uts`, the way Last.fm serves it with `extended=1`.
fn track(uts: i64) -> Value {
    json!({
        "artist": {"url": "https://www.last.fm/music/A", "name": "A", "image": [], "mbid": ""},
        "mbid": "", "name": format!("T{uts}"), "streamable": "0", "loved": "0",
        "url": "https://www.last.fm/music/A/_/T", "image": [],
        "album": {"mbid": "", "#text": "Al"},
        "date": {"uts": uts.to_string(), "#text": "14 Nov 2023, 22:13"},
    })
}

/// The backfill reads the last page first, commits oldest first and resumes the window next run.
#[tokio::test]
async fn backfill_pages_from_the_oldest_and_resumes() -> Result<()> {
    // 450 scrobbles are 3 pages; a run makes 3 requests in tests.
    let all: Vec<i64> = (T0..).step_by(300).take(450).collect();
    let (app, recorded, fake) = setup(all.clone()).await;
    tri!(Lastfm.hourly(&app).await);
    let first_run = queries(&fake);
    let pages: Vec<Option<&str>> = first_run
        .iter()
        .map(|query| query.get("page").map(String::as_str))
        .collect();
    ensure!(pages == [Some("1"), Some("3"), Some("2")], "page order");
    ensure!(
        first_run
            .first()
            .is_some_and(|query| !query.contains_key("from")),
        "the first request has no from"
    );
    let page3_newest = tri!(all.get(49).context("50th scrobble"));
    ensure!(
        first_run.get(2).and_then(|query| query.get("from"))
            == Some(&page3_newest.saturating_add(1).to_string()),
        "page 2 starts after page 3's newest"
    );
    ensure!(
        app.store.get::<i64>(LAST_KEY) == all.get(249).copied(),
        "last is the newest committed"
    );
    let window: Window = tri!(app.store.get(WINDOW_KEY).context("window"));
    ensure!(window.page == 1, "page 1 is left");
    let messages = lock(&recorded).commit_messages.clone();
    ensure!(messages == ["lastfm: hourly (2 files)"], "one commit");
    let at_t0 = tri!(DateTime::from_timestamp(T0, 0).context("T0"));
    let first_path = lock(&recorded)
        .trees
        .first()
        .and_then(|tree| tree.first())
        .cloned();
    ensure!(
        first_path == Some(storage::path(SOURCE, "recent_tracks", at_t0, None, "json")),
        "the first path"
    );
    let blobs = lock(&recorded).blobs.clone();
    ensure!(
        blobs.iter().all(|blob| !blob.contains("lastfm-key")),
        "the key stays out of the blobs"
    );
    ensure!(
        blobs.first().is_some_and(|blob| blob.contains(r#""request":{"method":"user.getrecenttracks","params":{"user":"alex","limit":"200","extended":"1","to":""#)),
        "the request envelope"
    );

    // A new scrobble after the window opened waits for the next window.
    lock(&fake).scrobbles.push(window.to.saturating_add(10));
    tri!(Lastfm.hourly(&app).await);
    let second_run = queries(&fake);
    ensure!(second_run.len() == 1, "one request");
    ensure!(
        second_run.first().and_then(|query| query.get("to")) == Some(&window.to.to_string()),
        "the same window"
    );
    ensure!(
        tri!(committed(&recorded)) == all,
        "every scrobble committed"
    );
    ensure!(
        app.store.get::<i64>(LAST_KEY) == all.last().copied(),
        "last is the newest"
    );
    ensure!(
        app.store.get::<Window>(WINDOW_KEY).is_none(),
        "the window closed"
    );
    Ok(())
}

/// Re-reading the lookback with nothing new makes one request and commits nothing.
#[tokio::test]
async fn rereading_the_lookback_commits_nothing_when_nothing_is_new() -> Result<()> {
    let recent = Utc::now().timestamp().saturating_sub(600);
    let (app, recorded, fake) = setup(vec![recent]).await;
    tri!(Lastfm.hourly(&app).await);
    // The single track comes as an object, not a list.
    let first_blob = lock(&recorded).blobs.first().cloned();
    let envelope: Value = tri!(serde_json::from_str(&tri!(
        first_blob.context("first blob")
    )));
    ensure!(
        envelope
            .pointer("/response/recenttracks/track")
            .is_some_and(Value::is_object),
        "a single track is an object"
    );
    drop(queries(&fake));
    tri!(Lastfm.hourly(&app).await);
    let second_run = queries(&fake);
    ensure!(second_run.len() == 1, "one request");
    ensure!(
        second_run.first().and_then(|query| query.get("from"))
            == Some(&recent.saturating_sub(LOOKBACK_SECS).to_string()),
        "from is the lookback before the newest"
    );
    ensure!(lock(&recorded).trees.len() == 1, "nothing new committed");
    ensure!(
        app.store.get::<i64>(LAST_KEY) == Some(recent),
        "last unchanged"
    );
    Ok(())
}

/// Scrobbles sent late but within the lookback are committed as a list of new tracks.
#[tokio::test]
async fn late_scrobbles_within_the_lookback_are_committed() -> Result<()> {
    let now = Utc::now().timestamp();
    let hour_ago = now.saturating_sub(3600);
    let half_hour_ago = now.saturating_sub(1800);
    let minute_ago = now.saturating_sub(60);
    let (app, recorded, fake) = setup(vec![hour_ago]).await;
    tri!(Lastfm.hourly(&app).await);
    // The Mac scrobbles a new play, then the iPhone sends an older one.
    lock(&fake).scrobbles.push(minute_ago);
    tri!(Lastfm.hourly(&app).await);
    ensure!(
        app.store.get::<i64>(LAST_KEY) == Some(minute_ago),
        "last moved to the new play"
    );
    lock(&fake).scrobbles.push(half_hour_ago);
    tri!(Lastfm.hourly(&app).await);
    let trees = lock(&recorded).trees.clone();
    ensure!(trees.len() == 3, "three commits");
    ensure!(
        trees
            .get(2)
            .and_then(|tree| tree.first())
            .is_some_and(|path| path.starts_with("raw/lastfm/scrobble/")),
        "late scrobbles go under scrobble/"
    );
    // Only the new scrobble, as a list of the verbatim items.
    let third = lock(&recorded).blobs.get(2).cloned();
    let envelope: Value = tri!(serde_json::from_str(&tri!(third.context("third blob"))));
    let response = tri!(
        envelope
            .get("response")
            .and_then(Value::as_array)
            .context("a list of tracks")
    );
    ensure!(response.len() == 1, "one new scrobble");
    ensure!(
        envelope.pointer("/response/0/name") == Some(&json!(format!("T{half_hour_ago}"))),
        "the late scrobble"
    );
    ensure!(
        envelope.pointer("/request/scrobbles") == Some(&json!(1_u64)),
        "the request counts one scrobble"
    );
    ensure!(
        tri!(committed(&recorded)) == [hour_ago, half_hour_ago, minute_ago],
        "all three committed"
    );
    ensure!(
        app.store.get::<i64>(LAST_KEY) == Some(minute_ago),
        "last is the newest"
    );
    let seen: HashMap<String, i64> = tri!(app.store.get(SEEN_KEY).context("seen"));
    ensure!(seen.len() == 3, "three scrobbles seen");
    Ok(())
}

/// The now playing track is skipped until it is scrobbled.
#[tokio::test]
async fn now_playing_is_ignored() -> Result<()> {
    let (app, recorded, fake) = setup(vec![]).await;
    lock(&fake).now_playing = true;
    tri!(Lastfm.hourly(&app).await);
    ensure!(lock(&recorded).trees.is_empty(), "nothing committed");
    ensure!(app.store.get::<i64>(LAST_KEY).is_none(), "no last");
    ensure!(app.store.get::<Window>(WINDOW_KEY).is_none(), "no window");
    lock(&fake).scrobbles.push(T0);
    tri!(Lastfm.hourly(&app).await);
    ensure!(
        tri!(committed(&recorded)) == [T0],
        "the scrobble is committed"
    );
    ensure!(
        app.store.get::<i64>(LAST_KEY) == Some(T0),
        "last is the scrobble"
    );
    Ok(())
}

/// Error 29 ends the run after saving progress; temporary errors are retried.
#[tokio::test]
async fn rate_limit_stops_the_run_and_keeps_progress() -> Result<()> {
    let all: Vec<i64> = (T0..).step_by(300).take(450).collect();
    let (app, recorded, fake) = setup(all.clone()).await;
    lock(&fake).limit_after = Some(2);
    tri!(Lastfm.hourly(&app).await);
    let messages = lock(&recorded).commit_messages.clone();
    ensure!(
        messages == ["lastfm: hourly (1 files)"],
        "one file committed"
    );
    ensure!(
        app.store.get::<i64>(LAST_KEY) == all.get(49).copied(),
        "last is page 3's newest"
    );
    ensure!(
        app.store
            .get::<Window>(WINDOW_KEY)
            .is_some_and(|window| window.page == 2),
        "page 2 is next"
    );

    // Temporary errors are retried; the next run resumes at page 2.
    lock(&fake).limit_after = None;
    lock(&fake).temporary = 2;
    drop(queries(&fake));
    tri!(Lastfm.hourly(&app).await);
    let pages: Vec<String> = queries(&fake)
        .into_iter()
        .filter_map(|mut query| query.remove("page"))
        .collect();
    ensure!(pages == ["2", "2", "2", "1"], "retries, then page 1");
    ensure!(
        tri!(committed(&recorded)) == all,
        "every scrobble committed"
    );
    Ok(())
}

/// Repeated temporary errors fail the run without the API key in the message.
#[tokio::test]
async fn errors_fail_without_the_key() -> Result<()> {
    let (app, _recorded, fake) = setup(vec![T0]).await;
    lock(&fake).temporary = 3;
    let run = Lastfm.hourly(&app).await;
    let Err(error) = run else {
        bail!("the run should fail");
    };
    let text = format!("{error:#}");
    ensure!(text.contains("user.getrecenttracks 500"), "{text}");
    ensure!(!text.contains("lastfm-key"), "{text}");
    ensure!(queries(&fake).len() == 3, "three attempts");
    Ok(())
}

/// The daily run commits the profile and every loved tracks page, only when they changed.
#[tokio::test]
async fn daily_commits_profile_and_loved_tracks_when_changed() -> Result<()> {
    let (app, recorded, fake) = setup(vec![T0]).await;
    lock(&fake).loved = "Song";
    tri!(Lastfm.daily(&app).await);
    tri!(Lastfm.daily(&app).await);
    let trees = lock(&recorded).trees.clone();
    ensure!(trees.len() == 1, "the unchanged second run commits nothing");
    let first_tree = tri!(trees.first().context("first tree"));
    let kinds: Vec<Option<&str>> = first_tree
        .iter()
        .map(|path| path.split('/').nth(2))
        .collect();
    ensure!(
        kinds
            == [
                Some("user_info"),
                Some("loved_tracks"),
                Some("loved_tracks")
            ],
        "kinds"
    );
    ensure!(
        first_tree
            .get(2)
            .is_some_and(|path| path.ends_with("-2.json")),
        "the second page has a suffix"
    );
    lock(&fake).loved = "";
    tri!(Lastfm.daily(&app).await);
    let after_three = lock(&recorded);
    ensure!(after_three.trees.len() == 2, "a changed loved list commits");
    ensure!(
        after_three.trees.get(1).is_some_and(|tree| tree.len() == 1),
        "only the loved tracks"
    );
    drop(after_three);
    Ok(())
}

/// Without a key and a user, both runs do nothing.
#[tokio::test]
async fn unconfigured_is_skipped() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app(&github);
    tri!(Lastfm.hourly(&app).await);
    tri!(Lastfm.daily(&app).await);
    ensure!(lock(&recorded).trees.is_empty(), "nothing committed");
    Ok(())
}
