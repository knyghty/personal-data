//! Pull every state change, logbook entry and long-term statistic from Home Assistant, plus daily registry snapshots.

use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, Utc};
use futures_util::SinkExt as _;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{info, warn};

use super::Source;
use crate::app::App;
use crate::storage::{self, File, count, floor_hour, windows};
use crate::tri;

/// File type and WebSocket command of each daily registry snapshot.
const DAILY_WS: &[(&str, &str)] = &[
    ("registry_entity", "config/entity_registry/list"),
    ("registry_device", "config/device_registry/list"),
    ("registry_area", "config/area_registry/list"),
    ("registry_floor", "config/floor_registry/list"),
    ("registry_label", "config/label_registry/list"),
    ("statistic_ids", "recorder/list_statistic_ids"),
];
/// Entities per history request.
const ENTITY_CHUNK: usize = 100;
/// Hour windows of history and logbook per run.
const HISTORY_WINDOWS_PER_RUN: usize = 48;
/// Directory name under `raw/`.
const SOURCE: &str = "home_assistant";
/// Day windows of statistics per run.
const STATS_WINDOWS_PER_RUN: usize = 30;

/// A configured Home Assistant: its URL and token with the app's HTTP client.
pub(super) struct Ha<'app> {
    /// The app, for its HTTP client, configuration and store.
    app: &'app App,
    /// The long-lived access token.
    token: String,
    /// The base URL, without a trailing slash.
    url: String,
}

/// A Home Assistant WebSocket session, authenticated on connect.
struct HaWs {
    /// The id of the next command.
    next_id: u64,
    /// The connection.
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

/// The Home Assistant source.
pub(super) struct HomeAssistant;

/// A WebSocket reply.
#[derive(Deserialize)]
struct WsMessage<'raw> {
    /// The error of a failed command.
    error: Option<Value>,
    /// The id of the command this answers.
    id: Option<u64>,
    /// The result of a successful command, verbatim.
    #[serde(borrow)]
    result: Option<&'raw RawValue>,
    /// Whether the command succeeded.
    success: Option<bool>,
    /// The message type, `result` for a command's answer.
    #[serde(rename = "type")]
    ty: String,
}

impl<'app> Ha<'app> {
    /// GET a path; a non-2xx reply is an error with the start of its body.
    async fn get(&self, path_and_query: &str) -> Result<String> {
        let url = format!("{}{path_and_query}", self.url);
        let resp = tri!(
            self.app
                .http
                .get(&url)
                .bearer_auth(&self.token)
                .send()
                .await
        );
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_success() {
            bail!(
                "GET {url} failed: {status} {}",
                text.chars().take(300).collect::<String>()
            );
        }
        Ok(text)
    }

    /// All state changes of all entities in `[start, end)`, merged into one file.
    async fn history(
        &self,
        ids: &[String],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<File>> {
        let start_param = urlencoding::encode(&start.to_rfc3339()).into_owned();
        let end_param = urlencoding::encode(&end.to_rfc3339()).into_owned();
        let chunks: Vec<String> = ids
            .chunks(ENTITY_CHUNK)
            .map(|chunk| chunk.join(","))
            .collect();
        let results: Vec<Result<String>> = stream::iter(chunks)
            .map(async |chunk| {
                let path = format!(
                    "/api/history/period/{start_param}?end_time={end_param}&filter_entity_id={chunk}&significant_changes_only=0&skip_initial_state"
                );
                self.get(&path).await
            })
            .buffer_unordered(4)
            .collect()
            .await;
        let mut merged: Vec<Vec<Box<RawValue>>> = Vec::new();
        let mut rows = 0_usize;
        for result in results {
            let lists: Vec<Vec<Box<RawValue>>> =
                tri!(serde_json::from_str(&tri!(result)).context("parse history"));
            for list in lists {
                if !list.is_empty() {
                    rows = rows.saturating_add(list.len());
                    merged.push(list);
                }
            }
        }
        if rows == 0 {
            return Ok(None);
        }
        let body = tri!(serde_json::to_string(&merged));
        Ok(Some(File {
            path: storage::path(SOURCE, "history", start, None, "json"),
            content: tri!(storage::envelope(
                now,
                json!({"start": start, "end": end, "entities": ids.len(), "rows": rows, "significant_changes_only": false, "skip_initial_state": true}),
                &body,
            )),
        }))
    }

    /// The logbook entries in `[start, end)`, as one file.
    async fn logbook(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<File>> {
        let path = format!(
            "/api/logbook/{}?end_time={}",
            urlencoding::encode(&start.to_rfc3339()),
            urlencoding::encode(&end.to_rfc3339())
        );
        let text = tri!(self.get(&path).await);
        let entries: Vec<&RawValue> = tri!(serde_json::from_str(&text).context("parse logbook"));
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(File {
            path: storage::path(SOURCE, "logbook", start, None, "json"),
            content: tri!(storage::envelope(
                now,
                json!({"start": start, "end": end, "entries": entries.len()}),
                &text,
            )),
        }))
    }

    /// The configured Home Assistant, when `HA_URL` and `HA_TOKEN` are set.
    pub(super) fn new(app: &'app App) -> Option<Self> {
        app.cfg
            .ha_url
            .clone()
            .zip(app.cfg.ha_token.clone())
            .map(|(url, token)| Self { app, token, url })
    }

    /// Send a notification through the notify `service`, e.g. `mobile_app_<phone>`.
    ///
    /// # Errors
    /// The service call fails or is refused.
    pub(super) async fn notify(&self, service: &str, title: &str, message: &str) -> Result<()> {
        let url = format!("{}/api/services/notify/{service}", self.url);
        let resp = tri!(
            self.app
                .http
                .post(&url)
                .bearer_auth(&self.token)
                .json(&json!({"title": title, "message": message}))
                .send()
                .await
        );
        let status = resp.status();
        if !status.is_success() {
            let text = tri!(resp.text().await);
            bail!(
                "POST {url} failed: {status} {}",
                text.chars().take(300).collect::<String>()
            );
        }
        Ok(())
    }

    /// Hourly statistics of every statistic id, one file per day window. Returns the files and the new cursor.
    async fn statistics(&self, now: DateTime<Utc>) -> Result<(Vec<File>, Option<DateTime<Utc>>)> {
        let cursor: DateTime<Utc> = tri!(
            self.app
                .store
                .get("home_assistant/cursor/stats")
                .or_else(|| {
                    now.checked_sub_signed(Duration::days(self.app.cfg.ha_stats_backfill_days))
                        .map(floor_hour)
                })
                .context("statistics start")
        );
        let stats_end = floor_hour(tri!(
            now.checked_sub_signed(Duration::minutes(20))
                .context("statistics end")
        ));
        let wins = windows(cursor, stats_end, Duration::days(1), STATS_WINDOWS_PER_RUN);
        let Some(&(_, last_end)) = wins.last() else {
            return Ok((Vec::new(), None));
        };
        let mut ws = tri!(self.ws().await);
        let (listed, _) = tri!(
            ws.call(json!({"type": "recorder/list_statistic_ids"}))
                .await
        );
        let stat_ids: Vec<&str> = listed.as_array().map_or_default(|items| {
            items
                .iter()
                .filter_map(|item| item.get("statistic_id").and_then(Value::as_str))
                .collect()
        });
        let mut files = Vec::new();
        for &(start, end) in &wins {
            if stat_ids.is_empty() {
                break;
            }
            let request = json!({
                "type": "recorder/statistics_during_period",
                "start_time": start,
                "end_time": end,
                "statistic_ids": stat_ids,
                "period": "hour",
            });
            let (value, raw) = tri!(ws.call(request).await);
            if count(&value) > 0 {
                files.push(File {
                    path: storage::path(SOURCE, "statistics", start, None, "json"),
                    content: tri!(storage::envelope(
                        now,
                        json!({"start_time": start, "end_time": end, "period": "hour", "statistic_ids": stat_ids.len()}),
                        &raw,
                    )),
                });
            }
        }
        Ok((files, Some(last_end)))
    }

    /// Remember the phone's position for the weather module.
    fn update_location(&self, states: &[Value], now: DateTime<Utc>) -> Result<()> {
        let is_gps = |state: &&Value| {
            state
                .get("entity_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with("device_tracker."))
                && state
                    .pointer("/attributes/source_type")
                    .and_then(Value::as_str)
                    == Some("gps")
                && state
                    .pointer("/attributes/latitude")
                    .is_some_and(Value::is_number)
        };
        let picked = self.app.cfg.ha_location_entity.as_deref().map_or_else(
            || states.iter().find(is_gps),
            |id| {
                states
                    .iter()
                    .find(|state| state.get("entity_id").and_then(Value::as_str) == Some(id))
            },
        );
        let Some(state) = picked else {
            warn!("home_assistant: no GPS device_tracker found");
            return Ok(());
        };
        let entity_id = state.get("entity_id").cloned().unwrap_or(Value::Null);
        let (Some(lat), Some(lon)) = (
            state
                .pointer("/attributes/latitude")
                .and_then(Value::as_f64),
            state
                .pointer("/attributes/longitude")
                .and_then(Value::as_f64),
        ) else {
            warn!("home_assistant: {entity_id} has no coordinates");
            return Ok(());
        };
        self.app.store.set(
            "location",
            &json!({
                "entity_id": entity_id,
                "latitude": lat,
                "longitude": lon,
                "gps_accuracy": state.pointer("/attributes/gps_accuracy"),
                "last_updated": state.get("last_updated"),
                "at": now,
            }),
        )
    }

    /// Open a WebSocket session and authenticate.
    async fn ws(&self) -> Result<HaWs> {
        let ws_url = format!(
            "{}/api/websocket",
            self.url
                .replacen("https://", "wss://", 1)
                .replacen("http://", "ws://", 1)
        );
        let (stream, _) = tri!(
            connect_async(&ws_url)
                .await
                .with_context(|| format!("connect {ws_url}"))
        );
        let mut ws = HaWs { next_id: 1, stream };
        let first = tri!(ws.recv().await);
        if !first.contains("auth_required") {
            bail!("unexpected first websocket message: {first}");
        }
        tri!(
            ws.send(json!({"type": "auth", "access_token": self.token}))
                .await
        );
        let reply = tri!(ws.recv().await);
        if !reply.contains("auth_ok") {
            bail!("websocket auth failed: {reply}");
        }
        Ok(ws)
    }
}

impl HaWs {
    /// Send a command and wait for its result. Returns the parsed result and its verbatim text.
    async fn call(&mut self, mut payload: Value) -> Result<(Value, String)> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let ty = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_owned();
        let command = tri!(
            payload
                .as_object_mut()
                .context("websocket command is not an object")
        );
        drop(command.insert("id".to_owned(), json!(id)));
        tri!(self.send(payload).await);
        loop {
            let text = tri!(self.recv().await);
            let msg: WsMessage<'_> =
                tri!(serde_json::from_str(&text).context("parse websocket message"));
            if msg.id != Some(id) || msg.ty != "result" {
                continue;
            }
            if msg.success != Some(true) {
                bail!("{ty} failed: {}", msg.error.unwrap_or(Value::Null));
            }
            let raw = msg
                .result
                .map_or_else(|| "null".to_owned(), |result| result.get().to_owned());
            let value: Value = tri!(serde_json::from_str(&raw));
            return Ok((value, raw));
        }
    }

    /// The next text message; control frames are skipped.
    async fn recv(&mut self) -> Result<String> {
        loop {
            let next = tri!(self.stream.next().await.context("websocket closed"));
            let message = tri!(next);
            match message {
                Message::Text(text) => return Ok(text.as_str().to_owned()),
                Message::Close(frame) => bail!("websocket closed by server: {frame:?}"),
                Message::Binary(_) | Message::Frame(_) | Message::Ping(_) | Message::Pong(_) => {}
            }
        }
    }

    /// Send a JSON message.
    async fn send(&mut self, payload: Value) -> Result<()> {
        tri!(
            self.stream
                .send(Message::Text(payload.to_string().into()))
                .await
        );
        Ok(())
    }
}

#[async_trait]
impl Source for HomeAssistant {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(ha) = Ha::new(app) else {
            return Ok(());
        };
        let now = Utc::now();
        let mut files = Vec::new();
        for (kind, path) in [("states", "/api/states"), ("config", "/api/config")] {
            let text = tri!(ha.get(path).await);
            files.push(File {
                path: storage::path(SOURCE, kind, now, None, "json"),
                content: tri!(storage::envelope(now, json!({"path": path}), &text)),
            });
        }
        let mut ws = tri!(ha.ws().await);
        for &(kind, command) in DAILY_WS {
            let (_, raw) = tri!(ws.call(json!({"type": command})).await);
            files.push(File {
                path: storage::path(SOURCE, kind, now, None, "json"),
                content: tri!(storage::envelope(now, json!({"type": command}), &raw)),
            });
        }
        drop(tri!(
            app.save(&files, "home_assistant: daily snapshots").await
        ));
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(ha) = Ha::new(app) else {
            info!("home_assistant: not configured, skipping");
            return Ok(());
        };
        let now = Utc::now();
        let states_text = tri!(ha.get("/api/states").await);
        let states: Vec<Value> =
            tri!(serde_json::from_str(&states_text).context("parse /api/states"));
        let ids: Vec<String> = states
            .iter()
            .filter_map(|state| {
                state
                    .get("entity_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
        tri!(ha.update_location(&states, now));

        let mut files = Vec::new();
        let mut cursors: Vec<(&str, DateTime<Utc>)> = Vec::new();

        let history_cursor: DateTime<Utc> = tri!(
            app.store
                .get("home_assistant/cursor/history")
                .or_else(|| {
                    now.checked_sub_signed(Duration::days(app.cfg.ha_backfill_days))
                        .map(floor_hour)
                })
                .context("history start")
        );
        let wins = windows(
            history_cursor,
            floor_hour(now),
            Duration::hours(1),
            HISTORY_WINDOWS_PER_RUN,
        );
        for &(start, end) in &wins {
            files.extend(tri!(ha.history(&ids, start, end, now).await));
            files.extend(tri!(ha.logbook(start, end, now).await));
        }
        if let Some(&(_, end)) = wins.last() {
            cursors.push(("home_assistant/cursor/history", end));
        }

        let (stat_files, stats_cursor) = tri!(ha.statistics(now).await);
        files.extend(stat_files);
        if let Some(end) = stats_cursor {
            cursors.push(("home_assistant/cursor/stats", end));
        }

        drop(tri!(
            app.save(
                &files,
                &format!("home_assistant: hourly ({} files)", files.len()),
            )
            .await
        ));
        for (key, value) in cursors {
            tri!(app.store.set(key, &value));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}
