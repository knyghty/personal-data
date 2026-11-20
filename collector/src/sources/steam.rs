//! Pull the owner's Steam account: hourly presence and recent playtime, achievements of played games, and daily account snapshots.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Utc};
use reqwest::{Error, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tracing::{info, warn};

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Games whose achievements are pulled per run, so the first backfill spreads over a few runs.
const ACHIEVEMENT_GAMES_PER_RUN: usize = 100;
/// Method listing the achievements of one game.
const ACHIEVEMENTS: &str = "ISteamUserStats/GetPlayerAchievements/v1";
/// Daily snapshots, committed only when the reply changed.
const DAILY: &[Call] = &[
    ("owned_games", OWNED_GAMES, "steamid", OWNED_PARAMS),
    ("badges", "IPlayerService/GetBadges/v1", "steamid", &[]),
    (
        "badge_progress",
        "IPlayerService/GetCommunityBadgeProgress/v1",
        "steamid",
        &[],
    ),
    (
        "friends",
        "ISteamUser/GetFriendList/v1",
        "steamid",
        &[("relationship", "friend")],
    ),
    ("bans", "ISteamUser/GetPlayerBans/v1", "steamids", &[]),
    ("groups", "ISteamUser/GetUserGroupList/v1", "steamid", &[]),
    (
        "wishlist",
        "IWishlistService/GetWishlist/v1",
        "steamid",
        &[],
    ),
];
/// Method listing the owned games.
const OWNED_GAMES: &str = "IPlayerService/GetOwnedGames/v1";
/// Parameters asking for every owned game with its details.
const OWNED_PARAMS: &[(&str, &str)] = &[
    ("include_appinfo", "1"),
    ("include_played_free_games", "1"),
    ("include_extended_appinfo", "1"),
];
/// Method returning the stats schema of one game.
const SCHEMA: &str = "ISteamUserStats/GetSchemaForGame/v2";
/// Directory name under `raw/`.
const SOURCE: &str = "steam";

/// The Web API for the configured user.
struct Api<'app> {
    /// The app whose HTTP client and configuration to use.
    app: &'app App,
    /// The 64-bit Steam id of the user.
    id: String,
    /// The Web API key.
    key: String,
}

impl<'app> Api<'app> {
    /// Call `method` for the configured user. Returns the status, the body and the request without the key.
    async fn call(
        &self,
        method: &str,
        id_param: &str,
        extra: &[(&str, &str)],
    ) -> Result<(StatusCode, String, Value)> {
        let mut params = vec![(id_param, self.id.as_str())];
        params.extend_from_slice(extra);
        let request = json!({
            "method": method,
            "params": params.iter().map(|&(name, value)| (name.to_owned(), json!(value))).collect::<Map<_, _>>(),
        });
        // The key is in the URL, so strip the URL from errors before they reach logs and issues.
        let response = tri!(
            self.app
                .http
                .get(format!("{}/{method}", self.app.cfg.steam_api_url))
                .query(&[("key", self.key.as_str())])
                .query(&params)
                .send()
                .await
                .map_err(Error::without_url)
        );
        let status = response.status();
        let text = tri!(response.text().await.map_err(Error::without_url));
        Ok((status, text, request))
    }

    /// Call `method` and fail on any status but 2xx. Returns the body and the request without the key.
    async fn get(
        &self,
        method: &str,
        id_param: &str,
        extra: &[(&str, &str)],
    ) -> Result<(String, Value)> {
        let (status, text, request) = tri!(self.call(method, id_param, extra).await);
        if !status.is_success() {
            bail!("{method} {status}: {}", snippet(&text));
        }
        Ok((text, request))
    }

    /// Whether the game's stats schema lists any achievements.
    async fn has_achievements(&self, appid: &str) -> Result<bool> {
        let (text, _) = tri!(self.get(SCHEMA, "steamid", &[("appid", appid)]).await);
        let schema: Value = tri!(serde_json::from_str(&text).context("parse game schema"));
        Ok(schema
            .pointer("/game/availableGameStats/achievements")
            .and_then(Value::as_array)
            .is_some_and(|achievements| !achievements.is_empty()))
    }

    /// The API for the configured key and user, if any.
    fn new(app: &'app App) -> Option<Self> {
        let Some(key) = app.cfg.steam_api_key.clone() else {
            return None;
        };
        let Some(id) = app.cfg.steam_id.clone() else {
            return None;
        };
        Some(Self { app, id, key })
    }
}

/// A daily snapshot: type, method, id parameter and extra parameters.
type Call = (
    &'static str,
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

/// One owned game.
#[derive(Deserialize)]
struct Game {
    /// Steam app id.
    appid: u64,
    /// Whether the game publishes stats, and so maybe achievements.
    #[serde(default)]
    has_community_visible_stats: bool,
    /// Minutes played.
    playtime_forever: u64,
}

/// The owned games reply.
#[derive(Deserialize)]
struct Owned {
    /// The reply body.
    response: OwnedGames,
}

impl Owned {
    /// The sorted owned appids, comma-separated.
    fn appids(&self) -> String {
        let mut ids: Vec<u64> = self.response.games.iter().map(|game| game.appid).collect();
        ids.sort_unstable();
        ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
    }

    /// Played games with stats whose playtime moved since their last achievement pull, games pulled before first.
    fn due(&self, seen: &BTreeMap<u64, Played>) -> Vec<(u64, u64)> {
        let mut games: Vec<(u64, u64)> = self
            .response
            .games
            .iter()
            .filter(|game| game.has_community_visible_stats && game.playtime_forever > 0)
            .filter(|game| {
                seen.get(&game.appid).map(|played| played.playtime) != Some(game.playtime_forever)
            })
            .map(|game| (game.appid, game.playtime_forever))
            .collect();
        games.sort_by_key(|&(appid, _)| !seen.contains_key(&appid));
        games.truncate(ACHIEVEMENT_GAMES_PER_RUN);
        games
    }
}

/// The body of the owned games reply.
#[derive(Deserialize)]
struct OwnedGames {
    /// Missing when game details are private.
    #[serde(default)]
    games: Vec<Game>,
}

/// Playtime and achievement reply hash at the last achievement pull of a game. `sha` is None for games without stats.
#[derive(Serialize, Deserialize)]
struct Played {
    /// Minutes played at the last pull.
    playtime: u64,
    /// Hash of the last achievement reply.
    sha: Option<String>,
}

/// The Steam source.
pub(super) struct Steam;

#[async_trait]
impl Source for Steam {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(api) = Api::new(app) else {
            return Ok(());
        };
        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for &(kind, method, id_param, extra) in DAILY {
            let (text, request) = tri!(api.get(method, id_param, extra).await);
            let key = format!("steam/sha/{kind}");
            // Playtime lives in `recently_played`, so the library is stored only when a game is added or removed.
            let fingerprint = if kind == "owned_games" {
                let owned: Owned = tri!(serde_json::from_str(&text));
                owned.appids()
            } else {
                text.clone()
            };
            if let Some(sha) = changed(app, &key, &fingerprint) {
                files.push(tri!(file(kind, now, None, request, &text)));
                hashes.push((key, sha));
            }
        }
        let message = format!("steam: daily ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (key, sha) in hashes {
            tri!(app.store.set(&key, &sha));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(api) = Api::new(app) else {
            info!("steam: not configured, skipping");
            return Ok(());
        };
        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();

        let (summary, summary_request) = tri!(
            api.get("ISteamUser/GetPlayerSummaries/v2", "steamids", &[])
                .await
        );
        files.push(tri!(file("summary", now, None, summary_request, &summary)));

        let (recent, recent_request) = tri!(
            api.get("IPlayerService/GetRecentlyPlayedGames/v1", "steamid", &[])
                .await
        );
        let recent_key = "steam/sha/recently_played";
        if let Some(recent_sha) = changed(app, recent_key, &recent) {
            files.push(tri!(file(
                "recently_played",
                now,
                None,
                recent_request,
                &recent
            )));
            hashes.push((recent_key.to_owned(), recent_sha));
        }

        let (owned_text, _) = tri!(api.get(OWNED_GAMES, "steamid", OWNED_PARAMS).await);
        let owned: Owned = tri!(serde_json::from_str(&owned_text));
        let mut seen: BTreeMap<u64, Played> =
            app.store.get("steam/achievements").unwrap_or_default();
        for (appid, playtime) in owned.due(&seen) {
            let id = appid.to_string();
            let (status, text, request) = tri!(
                api.call(ACHIEVEMENTS, "steamid", &[("appid", &id), ("l", "english")])
                    .await
            );
            let sha = if status.is_server_error() {
                if tri!(api.has_achievements(&id).await) {
                    // A real error: leave the game unseen to retry next hour, and pull the rest.
                    warn!("steam: {ACHIEVEMENTS} {appid} {status}: {}", snippet(&text));
                    continue;
                }
                // Steam answers 500, not 400, for games with stats but no achievements. Ask again once played again.
                None
            } else if status.is_success() {
                let digest = sha(&text);
                if seen.get(&appid).and_then(|played| played.sha.as_ref()) != Some(&digest) {
                    files.push(tri!(file("achievements", now, Some(&id), request, &text)));
                }
                Some(digest)
            } else if status.is_client_error()
                && !matches!(
                    status,
                    StatusCode::UNAUTHORIZED
                        | StatusCode::FORBIDDEN
                        | StatusCode::TOO_MANY_REQUESTS
                )
            {
                // Games without stats answer 400 "Requested app has no stats". Ask again once played again.
                None
            } else {
                bail!("{ACHIEVEMENTS} {appid} {status}: {}", snippet(&text));
            };
            drop(seen.insert(appid, Played { playtime, sha }));
        }

        let message = format!("steam: hourly ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (key, sha) in hashes {
            tri!(app.store.set(&key, &sha));
        }
        app.store.set("steam/achievements", &seen)
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// The hash of `body` when it differs from the last committed one under `key`.
pub(super) fn changed(app: &App, key: &str, body: &str) -> Option<String> {
    let digest = sha(body);
    (app.store.get::<String>(key).as_deref() != Some(digest.as_str())).then_some(digest)
}

/// A file of this source, holding `body` in an envelope.
fn file(
    kind: &str,
    at: DateTime<Utc>,
    suffix: Option<&str>,
    request: Value,
    body: &str,
) -> Result<File> {
    Ok(File {
        path: storage::path(SOURCE, kind, at, suffix, "json"),
        content: tri!(storage::envelope(at, request, body)),
    })
}

/// The hex SHA-256 of `body`.
fn sha(body: &str) -> String {
    format!("{:x}", Sha256::digest(body))
}

/// The first 300 characters of a reply, for errors.
fn snippet(text: &str) -> String {
    text.chars().take(300).collect()
}
