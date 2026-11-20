//! Drive the source against a fake Steam Web API and the fake GitHub.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::{DAILY, Played, Steam};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing::{self, Recorded};
use crate::tri;

/// The shared state of the fake Steam API.
type Fake = Arc<Mutex<Seen>>;

/// What the fake Steam saw, and the playtime of game 10 it reports.
#[derive(Default)]
struct Seen {
    /// Appids whose achievements answer 500 although their schema lists achievements.
    broken: Vec<&'static str>,
    /// Interface, method and appid of every call.
    calls: Vec<String>,
    /// Minutes played of game 10.
    playtime: u64,
    /// Appids with stats but no achievements, which Steam answers with 500.
    stats_only: Vec<&'static str>,
}

impl Seen {
    /// Answer one Web API call.
    fn answer(
        &mut self,
        iface: &str,
        method: &str,
        query: &HashMap<String, String>,
    ) -> (StatusCode, Json<Value>) {
        if query.get("key").map(String::as_str) != Some("steam-key") {
            return (StatusCode::UNAUTHORIZED, Json(json!({})));
        }
        let appid = query.get("appid").cloned().unwrap_or_default();
        self.calls.push(format!("{iface}/{method}{appid}"));
        let playtime = self.playtime;
        if method == "GetSchemaForGame" {
            let achievements = if self.stats_only.contains(&appid.as_str()) {
                json!([])
            } else {
                json!([{"name": "A"}])
            };
            return (
                StatusCode::OK,
                Json(
                    json!({"game": {"availableGameStats": {"stats": [{"name": "s"}], "achievements": achievements}}}),
                ),
            );
        }
        let fails =
            self.broken.contains(&appid.as_str()) || self.stats_only.contains(&appid.as_str());
        if fails && method == "GetPlayerAchievements" {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"playerstats": {"error": "Internal server error", "success": false}})),
            );
        }
        let body = match (method, appid.as_str()) {
            ("GetPlayerSummaries", _) => {
                if query.get("steamids").map(String::as_str) != Some("765") {
                    return (StatusCode::BAD_REQUEST, Json(json!({})));
                }
                json!({"response": {"players": [{"steamid": "765", "personastate": 1_u32, "gameid": "10"}]}})
            }
            ("GetRecentlyPlayedGames", _) => {
                json!({"response": {"total_count": 1_u32, "games": [{"appid": 10_u64, "playtime_2weeks": playtime, "playtime_forever": playtime}]}})
            }
            ("GetOwnedGames", _) => json!({"response": {"game_count": 4_u32, "games": [
                {"appid": 10_u64, "playtime_forever": playtime, "has_community_visible_stats": true},
                {"appid": 20_u64, "playtime_forever": 5_u64, "has_community_visible_stats": true},
                {"appid": 30_u64, "playtime_forever": 7_u64},
                {"appid": 40_u64, "playtime_forever": 0_u64, "has_community_visible_stats": true},
            ]}}),
            ("GetPlayerAchievements", "10") => {
                json!({"playerstats": {"steamID": "765", "gameName": "Ten", "achievements": [{"apiname": "A", "achieved": 1_u32, "unlocktime": 1_u64}], "success": true}})
            }
            ("GetPlayerAchievements", _) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"playerstats": {"error": "Requested app has no stats", "success": false}}),
                    ),
                );
            }
            _ => json!({"response": {}}),
        };
        (StatusCode::OK, Json(body))
    }
}

/// A 500 for a game with achievements is retried next run and does not stop the others.
#[tokio::test]
async fn a_server_error_is_retried_without_blocking_the_rest() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    lock(&fake).broken = vec!["10", "20"];
    tri!(Steam.hourly(&app).await);
    ensure!(
        last_kinds(&recorded) == ["summary", "recently_played"],
        "unexpected kinds"
    );
    let seen = tri!(stored(&app));
    ensure!(seen.is_empty(), "a broken game was marked seen");
    // Retried every run until it recovers.
    drop(achievement_calls(&fake));
    lock(&fake).broken.clear();
    tri!(Steam.hourly(&app).await);
    ensure!(
        achievement_calls(&fake)
            == [
                "ISteamUserStats/GetPlayerAchievements10",
                "ISteamUserStats/GetPlayerAchievements20"
            ],
        "unexpected achievement calls"
    );
    Ok(())
}

/// The achievement calls so far, which are then forgotten.
fn achievement_calls(fake: &Fake) -> Vec<String> {
    let mut seen = lock(fake);
    let out = seen
        .calls
        .iter()
        .filter(|call| call.contains("Achievements"))
        .cloned()
        .collect();
    seen.calls.clear();
    out
}

/// Daily snapshots are committed once, and again only when they change.
#[tokio::test]
async fn daily_commits_only_changed_snapshots() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Steam.daily(&app).await);
    lock(&fake).playtime = 90;
    tri!(Steam.daily(&app).await);
    ensure!(
        github(&recorded).trees.len() == 1,
        "a second commit was made"
    );
    ensure!(
        last_kinds(&recorded) == DAILY.iter().map(|call| call.0).collect::<Vec<_>>(),
        "unexpected kinds"
    );
    Ok(())
}

/// The first run commits presence, playtime and the achievements of every played game with stats.
#[tokio::test]
async fn first_run_pulls_presence_playtime_and_backfills_achievements() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Steam.hourly(&app).await);
    ensure!(
        last_kinds(&recorded) == ["summary", "recently_played", "achievements"],
        "unexpected kinds"
    );
    let record = github(&recorded);
    ensure!(
        record
            .trees
            .first()
            .and_then(|tree| tree.get(2))
            .is_some_and(|path| path.ends_with("-10.json")),
        "achievements not filed under the appid"
    );
    ensure!(
        record.blobs.iter().all(|blob| !blob.contains("steam-key")),
        "a blob leaks the key"
    );
    ensure!(
        record.blobs.first().is_some_and(|blob| blob.contains(
            r#""request":{"method":"ISteamUser/GetPlayerSummaries/v2","params":{"steamids":"765"}}"#
        )),
        "unexpected request envelope"
    );
    drop(record);
    ensure!(
        achievement_calls(&fake)
            == [
                "ISteamUserStats/GetPlayerAchievements10",
                "ISteamUserStats/GetPlayerAchievements20"
            ],
        "unexpected achievement calls"
    );
    let seen = tri!(stored(&app));
    ensure!(
        seen.keys().copied().collect::<Vec<_>>() == [10, 20],
        "unexpected seen games"
    );
    ensure!(
        seen.get(&20).is_some_and(|played| played.sha.is_none()),
        "game 20 has a hash"
    );
    Ok(())
}

/// Games with stats but no achievements are asked again only once played again.
#[tokio::test]
async fn games_without_achievements_wait_until_played_again() -> Result<()> {
    let (app, _, fake) = setup().await;
    lock(&fake).stats_only = vec!["20"];
    tri!(Steam.hourly(&app).await);
    let seen = tri!(stored(&app));
    ensure!(
        seen.keys().collect::<Vec<_>>() == [&10, &20],
        "unexpected seen games"
    );
    ensure!(
        seen.get(&20).is_some_and(|played| played.sha.is_none()),
        "game 20 has a hash"
    );
    drop(achievement_calls(&fake));
    tri!(Steam.hourly(&app).await);
    ensure!(
        achievement_calls(&fake).is_empty(),
        "an unplayed game was asked again"
    );
    Ok(())
}

/// Lock the fake GitHub's record, poisoned or not.
fn github(recorded: &testing::Shared) -> MutexGuard<'_, Recorded> {
    recorded.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Types of the files in the last commit.
fn last_kinds(recorded: &testing::Shared) -> Vec<String> {
    github(recorded)
        .trees
        .last()
        .into_iter()
        .flatten()
        .map(|path| path.split('/').nth(2).unwrap_or_default().to_owned())
        .collect()
}

/// Later runs pull achievements only for games played since, and commit only changed replies.
#[tokio::test]
async fn later_runs_pull_only_played_games_and_commit_changes() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Steam.hourly(&app).await);
    drop(achievement_calls(&fake));
    tri!(Steam.hourly(&app).await);
    ensure!(last_kinds(&recorded) == ["summary"], "unexpected kinds");
    ensure!(
        achievement_calls(&fake).is_empty(),
        "achievements pulled without play"
    );
    lock(&fake).playtime = 90;
    tri!(Steam.hourly(&app).await);
    ensure!(
        last_kinds(&recorded) == ["summary", "recently_played"],
        "unexpected kinds after play"
    );
    ensure!(
        achievement_calls(&fake) == ["ISteamUserStats/GetPlayerAchievements10"],
        "unexpected achievement calls"
    );
    Ok(())
}

/// Lock the fake's state, poisoned or not.
fn lock(fake: &Fake) -> MutexGuard<'_, Seen> {
    fake.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Start a fake Steam API and an `App` pointed at it and at the fake GitHub.
async fn setup() -> (Arc<App>, testing::Shared, Fake) {
    let (github_url, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(Seen {
        playtime: 60,
        ..Seen::default()
    }));
    let router = Router::new()
        .route(
            "/{iface}/{method}/{ver}",
            get(
                async |State(state): State<Fake>,
                       Path((iface, method, _)): Path<(String, String, String)>,
                       Query(query): Query<HashMap<String, String>>| {
                    lock(&state).answer(&iface, &method, &query)
                },
            ),
        )
        .with_state(Arc::clone(&fake));
    let url = testing::serve(router).await;
    let app = testing::app_with(&github_url, |cfg| {
        cfg.steam_api_key = Some("steam-key".into());
        cfg.steam_id = Some("765".into());
        cfg.steam_api_url = url;
    });
    (app, recorded, fake)
}

/// The stored achievement pulls.
fn stored(app: &App) -> Result<BTreeMap<u64, Played>> {
    let seen = tri!(
        app.store
            .get("steam/achievements")
            .context("no achievement pulls stored")
    );
    Ok(seen)
}

/// Without a key and id both jobs do nothing.
#[tokio::test]
async fn unconfigured_is_skipped() -> Result<()> {
    let (github_url, recorded) = testing::fake_github().await;
    let app = testing::app(&github_url);
    tri!(Steam.hourly(&app).await);
    tri!(Steam.daily(&app).await);
    ensure!(
        github(&recorded).trees.is_empty(),
        "something was committed"
    );
    Ok(())
}
