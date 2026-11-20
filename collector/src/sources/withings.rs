//! Pull every Withings data type, with `OAuth2` tokens in the store and webhook notifications for fast updates.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::time::Duration;
use std::collections::HashMap;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Redirect, Response};
use axum::routing::{get, post};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

use super::Source;
use crate::app::App;
use crate::storage::{self, File, count};
use crate::tri;

/// Daily activity summaries.
const ACTIVITY: Spec = Spec {
    action: "getactivity",
    fields: Some(ACTIVITY_FIELDS),
    items: "activities",
    kind: "activity",
    path: "/v2/measure",
};
/// Fields of an activity summary.
const ACTIVITY_FIELDS: &str = "steps,distance,elevation,soft,moderate,intense,active,calories,totalcalories,hr_average,hr_min,hr_max,hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3";
/// Origin of the data API.
const API: &str = "https://wbsapi.withings.net";
/// Notification categories to subscribe to (see the Withings notification categories page).
const APPLIS: &[u32] = &[1, 2, 4, 16, 44, 46, 50, 51, 52, 54, 55, 58, 62];
/// The authorisation page.
const AUTHORIZE: &str = "https://account.withings.com/oauth2_user/authorize2";
/// Seconds in a day.
const DAY: i64 = 86_400;
/// ECG recordings.
const HEART: Spec = Spec {
    action: "list",
    fields: None,
    items: "series",
    kind: "heart",
    path: "/v2/heart",
};
/// Intraday activity, 24 h per call.
const INTRADAY: Spec = Spec {
    action: "getintradayactivity",
    fields: None,
    items: "series",
    kind: "intraday_activity",
    path: "/v2/measure",
};
/// Body measurements.
const MEASURE: Spec = Spec {
    action: "getmeas",
    fields: None,
    items: "measuregrps",
    kind: "measure",
    path: "/measure",
};
/// Serialises pulls, so a notification and the hourly job never read a cursor the other is about to move.
static PULL_LOCK: Mutex<()> = Mutex::const_new(());
/// Scopes to ask for.
const SCOPES: &str = "user.info,user.metrics,user.activity,user.sleepevents";
/// Sleep series, 24 h per call.
const SLEEP: Spec = Spec {
    action: "get",
    fields: Some(SLEEP_FIELDS),
    items: "series",
    kind: "sleep",
    path: "/v2/sleep",
};
/// Fields of a sleep series.
const SLEEP_FIELDS: &str = "hr,rr,snoring,sdnn_1,rmssd,hrv_quality,mvt_score,chest_movement_rate,withings_index,breathing_sounds";
/// Nightly sleep summaries.
const SLEEP_SUMMARY: Spec = Spec {
    action: "getsummary",
    fields: Some(SLEEP_SUMMARY_FIELDS),
    items: "series",
    kind: "sleep_summary",
    path: "/v2/sleep",
};
/// Fields of a sleep summary.
const SLEEP_SUMMARY_FIELDS: &str = "total_timeinbed,total_sleep_time,asleepduration,lightsleepduration,remsleepduration,deepsleepduration,sleep_efficiency,sleep_latency,wakeup_latency,wakeupduration,wakeupcount,waso,nb_rem_episodes,breathing_disturbances_intensity,apnea_hypopnea_index,withings_index,durationtosleep,durationtowakeup,out_of_bed_count,hr_average,hr_min,hr_max,rr_average,rr_min,rr_max,breathing_quality_assessment,snoring,snoringepisodecount,sleep_score,night_events,mvt_score_avg,mvt_active_duration,rmssd_start_avg,rmssd_end_avg,chest_movement_rate_wellness_average,chest_movement_rate_wellness_min,chest_movement_rate_wellness_max,breathing_sounds,breathing_sounds_episode_count,chest_movement_rate_average,chest_movement_rate_min,chest_movement_rate_max,core_body_temperature_min,core_body_temperature_max,core_body_temperature_avg,core_body_temperature_status";
/// Directory name under `raw/`.
const SOURCE: &str = "withings";
/// Most 24 h windows of intraday activity or sleep per run.
const WINDOW_CAP: usize = 200;
/// Windows per commit. Each sleep window can be 1 MB, so commit often to bound memory and keep progress.
const WINDOW_CHUNK: usize = 20;
/// Workouts.
const WORKOUTS: Spec = Spec {
    action: "getworkouts",
    fields: Some(WORKOUT_FIELDS),
    items: "series",
    kind: "workouts",
    path: "/v2/measure",
};
/// Fields of a workout.
const WORKOUT_FIELDS: &str = "calories,intensity,manual_intensity,manual_distance,manual_calories,hr_average,hr_min,hr_max,hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3,pause_duration,algo_pause_duration,spo2_average,core_body_temperature_avg,core_body_temperature_min,core_body_temperature_max,core_body_temperature_status,steps,distance,elevation,pool_laps,strokes,pool_length";

/// The data API for the configured `OAuth2` client.
struct Client<'app> {
    /// The app whose HTTP client, store and configuration to use.
    app: &'app App,
    /// The `OAuth2` client id.
    id: String,
    /// The `OAuth2` client secret.
    secret: String,
}

impl<'app> Client<'app> {
    /// Where the first pull starts: `WITHINGS_BACKFILL_FROM`, else a year ago.
    fn backfill_from(&self) -> DateTime<Utc> {
        self.app
            .cfg
            .withings_backfill_from
            .as_deref()
            .and_then(|text| text.parse::<NaiveDate>().ok())
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map_or_else(
                || {
                    Utc::now()
                        .checked_sub_signed(TimeDelta::days(365))
                        .unwrap_or(DateTime::UNIX_EPOCH)
                },
                |midnight| midnight.and_utc(),
            )
    }

    /// Call a data endpoint. Returns the parsed reply and its verbatim text.
    async fn call(&self, path: &str, params: &[(&str, String)]) -> Result<(Value, String)> {
        let mut refreshed = false;
        let mut waited = false;
        loop {
            let current = tri!(self.tokens().await);
            let response = tri!(
                self.app
                    .http
                    .post(format!("{API}{path}"))
                    .bearer_auth(&current.access_token)
                    .form(params)
                    .send()
                    .await
            );
            let text = tri!(response.text().await);
            let reply: Value = tri!(serde_json::from_str(&text).with_context(|| {
                format!(
                    "withings {path}: not JSON: {}",
                    text.chars().take(200).collect::<String>()
                )
            }));
            match reply.get("status").and_then(Value::as_i64).unwrap_or(-1) {
                0 => return Ok((reply, text)),
                401 if !refreshed => {
                    refreshed = true;
                    drop(tri!(self.refresh(&current.refresh_token).await));
                }
                601 if !waited => {
                    waited = true;
                    warn!("withings: rate limited, waiting 60s");
                    sleep(Duration::from_mins(1)).await;
                }
                status => {
                    let query = params
                        .iter()
                        .map(|param| format!("{}={}", param.0, param.1))
                        .collect::<Vec<_>>()
                        .join(" ");
                    bail!(
                        "withings {path} {query} failed: status {status}: {}",
                        reply.get("error").unwrap_or(&Value::Null)
                    )
                }
            }
        }
    }

    /// Commit the pulled files, then advance the cursors.
    async fn commit(&self, pull: Pull, message: &str) -> Result<()> {
        drop(tri!(self.app.save(&pull.files, message).await));
        for (key, value) in pull.cursors {
            tri!(self.app.store.set(&key, &value));
        }
        Ok(())
    }

    /// Exchange the login code for tokens, forget the state and subscribe to notifications. Returns the user id.
    async fn connect(&self, code: &str) -> Result<Value> {
        let tokens = tri!(
            self.token_request(&[
                ("grant_type", "authorization_code".into()),
                ("code", code.to_owned()),
                (
                    "redirect_uri",
                    format!("{}/withings/callback", self.app.cfg.public_url),
                ),
            ])
            .await
        );
        if let Err(error) = self.app.store.remove("withings/oauth_state") {
            warn!("withings: forgetting the oauth state failed: {error:#}");
        }
        self.subscribe_all().await;
        Ok(tokens.userid)
    }

    /// Pull what a notification refers to, when it is for the authorised user.
    async fn handle_notification(
        &self,
        received: DateTime<Utc>,
        notification: Notification,
    ) -> Result<()> {
        let tokens: Option<Tokens> = self.app.store.get("withings/tokens");
        let own = tokens
            .as_ref()
            .map(|current| current.userid.to_string().trim_matches('"').to_owned());
        if own.is_none() || notification.0.get("userid") != own.as_ref() {
            warn!("withings: notification for unknown user, ignored");
            return Ok(());
        }
        let _guard = PULL_LOCK.lock().await;
        let mut pull = Pull::default();
        pull.files.push(File {
            path: storage::path(SOURCE, "notification", received, None, "json"),
            content: tri!(serde_json::to_string(
                &json!({"received_at": received, "fields": notification.0})
            )),
        });
        let appli: u32 = notification
            .0
            .get("appli")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let (start, end) = notification.window(received);
        pull.merge(tri!(self.pull_for_notification(appli, start, end).await));
        let message = format!("withings: notification appli {appli}");
        tri!(self.commit(pull, &message).await);
        Ok(())
    }

    /// The client for the configured id and secret, if any.
    fn new(app: &'app App) -> Option<Self> {
        let Some(id) = app.cfg.withings_client_id.clone() else {
            return None;
        };
        let Some(secret) = app.cfg.withings_client_secret.clone() else {
            return None;
        };
        Some(Self { app, id, secret })
    }

    /// Pull the types a notification category refers to, for the notified window.
    async fn pull_for_notification(&self, appli: u32, start: i64, end: i64) -> Result<Pull> {
        let mut pull = Pull::default();
        match appli {
            1 | 2 | 4 | 58 | 62 => pull.merge(tri!(self.pull_since(&MEASURE).await)),
            54 | 55 => {
                pull.merge(tri!(self.pull_since(&MEASURE).await));
                pull.merge(tri!(
                    self.pull_heart(Some((start.saturating_sub(3600), end.saturating_add(3600))))
                        .await
                ));
            }
            16 => {
                pull.merge(tri!(self.pull_since(&ACTIVITY).await));
                pull.merge(tri!(self.pull_since(&WORKOUTS).await));
                pull.merge(tri!(
                    self.pull_windows(&INTRADAY, Some((start, end)), usize::MAX)
                        .await
                ));
            }
            44 => {
                pull.merge(tri!(self.pull_since(&SLEEP_SUMMARY).await));
                pull.merge(tri!(
                    self.pull_windows(&SLEEP, Some((start, end)), usize::MAX)
                        .await
                ));
            }
            _ => {}
        }
        Ok(pull)
    }

    /// List ECG recordings and fetch the signal of every recording not seen before.
    async fn pull_heart(&self, window: Option<(i64, i64)>) -> Result<Pull> {
        let key = format!("withings/cursor/{}", HEART.kind);
        let now = Utc::now();
        let (start, end) = window.unwrap_or_else(|| {
            let from: i64 = self
                .app
                .store
                .get(&key)
                .unwrap_or_else(|| self.backfill_from().timestamp());
            (from, now.timestamp())
        });
        let mut seen: Vec<i64> = self
            .app
            .store
            .get("withings/seen_signals")
            .unwrap_or_default();
        let mut pull = Pull::default();
        let mut offset: Option<i64> = None;
        let mut page = 0_u32;
        loop {
            let mut params = vec![
                ("action", HEART.action.to_owned()),
                ("startdate", start.to_string()),
                ("enddate", end.to_string()),
            ];
            if let Some(from) = offset {
                params.push(("offset", from.to_string()));
            }
            let (reply, text) = tri!(self.call(HEART.path, &params).await);
            let body = reply.get("body").unwrap_or(&Value::Null);
            let series = body
                .get(HEART.items)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if !series.is_empty() {
                let at = tri!(DateTime::from_timestamp(start, 0).context("window start"));
                let suffix = (page > 0).then(|| page.to_string());
                pull.files.push(File {
                    path: storage::path(SOURCE, HEART.kind, at, suffix.as_deref(), "json"),
                    content: tri!(storage::envelope(now, json!({"action": HEART.action, "startdate": start, "enddate": end, "offset": offset}), &text)),
                });
            }
            for item in &series {
                let Some(id) = item.pointer("/ecg/signalid").and_then(Value::as_i64) else {
                    continue;
                };
                if seen.contains(&id) {
                    continue;
                }
                let (_, signal) = tri!(
                    self.call(
                        HEART.path,
                        &[("action", "get".into()), ("signalid", id.to_string())],
                    )
                    .await
                );
                let at = item
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
                    .unwrap_or(now);
                pull.files.push(File {
                    path: storage::path(SOURCE, "heart_signal", at, Some(&id.to_string()), "json"),
                    content: tri!(storage::envelope(
                        now,
                        json!({"action": "get", "signalid": id, "record": item}),
                        &signal,
                    )),
                });
                seen.push(id);
            }
            let more = body.get("more").is_some_and(|flag| {
                flag.as_bool().unwrap_or(false) || flag.as_i64().unwrap_or(0) != 0
            });
            match (more, body.get("offset").and_then(Value::as_i64)) {
                (true, Some(next)) if Some(next) != offset => {
                    offset = Some(next);
                    page = page.saturating_add(1);
                }
                _ => break,
            }
        }
        let excess = seen.len().saturating_sub(500);
        drop(seen.drain(..excess));
        pull.cursors
            .push(("withings/seen_signals".into(), json!(seen)));
        if window.is_none() {
            pull.cursors.push((key, json!(end)));
        }
        Ok(pull)
    }

    /// Pull a type that supports `lastupdate`, following pagination.
    async fn pull_since(&self, spec: &Spec) -> Result<Pull> {
        let key = format!("withings/cursor/{}", spec.kind);
        let since: i64 = self.app.store.get(&key).unwrap_or(1);
        let started = Utc::now();
        let mut pull = Pull::default();
        let mut offset: Option<i64> = None;
        let mut page = 0_u32;
        loop {
            let mut params = vec![
                ("action", spec.action.to_owned()),
                ("lastupdate", since.to_string()),
            ];
            if let Some(fields) = spec.fields {
                params.push(("data_fields", fields.to_owned()));
            }
            if let Some(from) = offset {
                params.push(("offset", from.to_string()));
            }
            let (reply, text) = tri!(self.call(spec.path, &params).await);
            let body = reply.get("body").unwrap_or(&Value::Null);
            if body.get(spec.items).map_or(0, count) > 0 {
                let suffix = (page > 0).then(|| page.to_string());
                pull.files.push(File {
                    path: storage::path(SOURCE, spec.kind, started, suffix.as_deref(), "json"),
                    content: tri!(storage::envelope(
                        started,
                        json!({"action": spec.action, "lastupdate": since, "offset": offset}),
                        &text,
                    )),
                });
            }
            let more = body.get("more").is_some_and(|flag| {
                flag.as_bool().unwrap_or(false) || flag.as_i64().unwrap_or(0) != 0
            });
            match (more, body.get("offset").and_then(Value::as_i64)) {
                (true, Some(next)) if Some(next) != offset => {
                    offset = Some(next);
                    page = page.saturating_add(1);
                }
                _ => break,
            }
        }
        pull.cursors.push((key, json!(started.timestamp())));
        Ok(pull)
    }

    /// Pull 24 h windows. With `window` set, pull exactly that range. Otherwise pull from the stored cursor, at most
    /// `cap` windows.
    async fn pull_windows(
        &self,
        spec: &Spec,
        window: Option<(i64, i64)>,
        cap: usize,
    ) -> Result<Pull> {
        let key = format!("withings/cursor/{}", spec.kind);
        let now = Utc::now();
        let (start, end) = window.unwrap_or_else(|| {
            let from: i64 = self
                .app
                .store
                .get(&key)
                .unwrap_or_else(|| self.backfill_from().timestamp());
            (from, now.timestamp())
        });
        let mut pull = Pull::default();
        let mut cursor = start;
        while cursor < end && pull.windows < cap {
            let until = cursor.saturating_add(DAY).min(end);
            let mut params = vec![
                ("action", spec.action.to_owned()),
                ("startdate", cursor.to_string()),
                ("enddate", until.to_string()),
            ];
            if let Some(fields) = spec.fields {
                params.push(("data_fields", fields.to_owned()));
            }
            let (reply, text) = tri!(self.call(spec.path, &params).await);
            if reply
                .get("body")
                .and_then(|body| body.get(spec.items))
                .map_or(0, count)
                > 0
            {
                let at = tri!(DateTime::from_timestamp(cursor, 0).context("window start"));
                pull.files.push(File {
                    path: storage::path(SOURCE, spec.kind, at, None, "json"),
                    content: tri!(storage::envelope(
                        now,
                        json!({"action": spec.action, "startdate": cursor, "enddate": until}),
                        &text,
                    )),
                });
            }
            cursor = until;
            pull.windows = pull.windows.saturating_add(1);
        }
        if window.is_none() {
            pull.cursors.push((key, json!(cursor)));
        }
        Ok(pull)
    }

    /// Refresh the tokens and return the new ones.
    async fn refresh(&self, refresh_token: &str) -> Result<Tokens> {
        info!("withings: refreshing access token");
        self.token_request(&[
            ("grant_type", "refresh_token".into()),
            ("refresh_token", refresh_token.to_owned()),
        ])
        .await
    }

    /// Subscribe the notification callback to every category in `APPLIS`, logging failures.
    async fn subscribe_all(&self) {
        let callback = format!("{}/withings/notify", self.app.cfg.public_url);
        for appli in APPLIS {
            let params = [
                ("action", "subscribe".to_owned()),
                ("callbackurl", callback.clone()),
                ("appli", appli.to_string()),
                ("comment", "personal-data".to_owned()),
            ];
            match self.call("/notify", &params).await {
                Ok(_) => info!("withings: subscribed appli {appli}"),
                Err(error) => warn!("withings: subscribe appli {appli} failed: {error:#}"),
            }
        }
    }

    /// Exchange a code or a refresh token for new tokens and persist them before returning.
    async fn token_request(&self, params: &[(&str, String)]) -> Result<Tokens> {
        let mut form: Vec<(&str, String)> = vec![
            ("action", "requesttoken".into()),
            ("client_id", self.id.clone()),
            ("client_secret", self.secret.clone()),
        ];
        form.extend(params.iter().cloned());
        let response = tri!(
            self.app
                .http
                .post(format!("{API}/v2/oauth2"))
                .form(&form)
                .send()
                .await
        );
        let reply: Value = tri!(response.json().await.context("withings token response"));
        if reply.get("status").and_then(Value::as_i64) != Some(0) {
            bail!("withings token request failed: {reply}");
        }
        let body = reply.get("body").unwrap_or(&Value::Null);
        let expires_in = body
            .get("expires_in")
            .and_then(Value::as_i64)
            .unwrap_or(10_800);
        let tokens = Tokens {
            access_token: tri!(
                body.get("access_token")
                    .and_then(Value::as_str)
                    .context("access_token")
            )
            .to_owned(),
            expires_at: Utc::now()
                .timestamp()
                .saturating_add(expires_in)
                .saturating_sub(120),
            refresh_token: tri!(
                body.get("refresh_token")
                    .and_then(Value::as_str)
                    .context("refresh_token")
            )
            .to_owned(),
            userid: body.get("userid").cloned().unwrap_or_default(),
        };
        tri!(self.app.store.set("withings/tokens", &tokens));
        Ok(tokens)
    }

    /// The stored tokens, refreshed when expired.
    async fn tokens(&self) -> Result<Tokens> {
        let stored: Tokens = tri!(
            self.app
                .store
                .get("withings/tokens")
                .context("withings is not authorised, open /withings/auth")
        );
        if Utc::now().timestamp() >= stored.expires_at {
            return self.refresh(&stored.refresh_token).await;
        }
        Ok(stored)
    }
}

/// The form fields Withings posts: `userid`, `appli`, `startdate`, `enddate` or `date`.
struct Notification(HashMap<String, String>);

impl Notification {
    /// Pull what the notification refers to, logging a failure.
    async fn handle(self, app: Arc<App>, received: DateTime<Utc>) {
        let Some(client) = Client::new(&app) else {
            return;
        };
        if let Err(error) = client.handle_notification(received, self).await {
            warn!("withings: notification handling failed: {error:#}");
        }
    }

    /// The time range the notification refers to: its `startdate`/`enddate`, else the day around its `date`, else
    /// the last 24 h.
    fn window(&self, received: DateTime<Utc>) -> (i64, i64) {
        let start = self
            .0
            .get("startdate")
            .and_then(|value| value.parse::<i64>().ok());
        let end = self
            .0
            .get("enddate")
            .and_then(|value| value.parse::<i64>().ok());
        let day = self
            .0
            .get("date")
            .and_then(|value| value.parse::<NaiveDate>().ok())
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|midnight| midnight.and_utc().timestamp());
        match (start, end, day) {
            (Some(from), Some(until), _) => (from, until),
            (_, _, Some(midnight)) => (
                midnight.saturating_sub(DAY),
                midnight.saturating_add(DAY.saturating_mul(2)),
            ),
            _ => (
                received.timestamp().saturating_sub(DAY),
                received.timestamp(),
            ),
        }
    }
}

/// Files produced by a pull plus the cursor updates to apply after they are committed.
#[derive(Default)]
struct Pull {
    /// Store keys and values to set once the files are committed.
    cursors: Vec<(String, Value)>,
    /// Files to commit.
    files: Vec<File>,
    /// Number of 24 h windows fetched, so callers know when the backfill is caught up.
    windows: usize,
}

impl Pull {
    /// Take over the files, cursors and window count of `other`.
    fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
        self.cursors.extend(other.cursors);
        self.windows = self.windows.saturating_add(other.windows);
    }
}

/// One data type: where it lives and which response key holds the items.
struct Spec {
    /// The `action` parameter.
    action: &'static str,
    /// The `data_fields` parameter, if the action takes one.
    fields: Option<&'static str>,
    /// The body key holding the items.
    items: &'static str,
    /// Directory name under `raw/withings/`.
    kind: &'static str,
    /// Endpoint path.
    path: &'static str,
}

/// The stored tokens.
#[derive(Serialize, Deserialize)]
struct Tokens {
    /// Bearer token for the API.
    access_token: String,
    /// Unix time at which `access_token` expires, with a margin.
    expires_at: i64,
    /// Token for the next refresh.
    refresh_token: String,
    /// The Withings user id, as the token reply gives it.
    userid: Value,
}

/// The Withings source.
pub(super) struct Withings;

#[async_trait]
impl Source for Withings {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(client) = Client::new(app) else {
            return Ok(());
        };
        if app.store.get::<Tokens>("withings/tokens").is_none() {
            return Ok(());
        }
        let now = Utc::now();
        let mut files = Vec::new();
        for (kind, action) in [("user_devices", "getdevice"), ("user_goals", "getgoals")] {
            let (_, text) = tri!(
                client
                    .call("/v2/user", &[("action", action.to_owned())])
                    .await
            );
            files.push(File {
                path: storage::path(SOURCE, kind, now, None, "json"),
                content: tri!(storage::envelope(now, json!({"action": action}), &text)),
            });
        }
        drop(tri!(app.save(&files, "withings: daily").await));
        client.subscribe_all().await;
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(client) = Client::new(app) else {
            info!("withings: not configured, skipping");
            return Ok(());
        };
        let _guard = PULL_LOCK.lock().await;
        if app.store.get::<Tokens>("withings/tokens").is_none() {
            info!("withings: not authorised yet, open /withings/auth");
            return Ok(());
        }
        let mut pull = Pull::default();
        for spec in [&MEASURE, &ACTIVITY, &WORKOUTS, &SLEEP_SUMMARY] {
            pull.merge(tri!(client.pull_since(spec).await));
        }
        pull.merge(tri!(client.pull_heart(None).await));
        tri!(client.commit(pull, "withings: hourly").await);
        for spec in [&INTRADAY, &SLEEP] {
            let mut budget = WINDOW_CAP;
            while budget > 0 {
                let cap = WINDOW_CHUNK.min(budget);
                let chunk = tri!(client.pull_windows(spec, None, cap).await);
                let fetched = chunk.windows;
                let message = format!("withings: {}", spec.kind);
                tri!(client.commit(chunk, &message).await);
                budget = budget.saturating_sub(fetched);
                if fetched < cap {
                    break;
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        // Send the browser to Withings with a fresh state.
        let auth = async |State(app): State<Arc<App>>,
                          Query(query): Query<HashMap<String, String>>|
               -> Response {
            if query.get("key") != Some(&app.cfg.admin_secret) {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let Some(client_id) = app.cfg.withings_client_id.as_deref() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "WITHINGS_CLIENT_ID is not set",
                )
                    .into_response();
            };
            let state = random_state();
            if let Err(error) = app.store.set("withings/oauth_state", &state) {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response();
            }
            let redirect_uri = format!("{}/withings/callback", app.cfg.public_url);
            let url = Url::parse_with_params(
                AUTHORIZE,
                [
                    ("response_type", "code"),
                    ("client_id", client_id),
                    ("scope", SCOPES),
                    ("redirect_uri", &redirect_uri),
                    ("state", &state),
                ],
            );
            url.map_or_else(
                |error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("authorize url: {error}"),
                    )
                        .into_response()
                },
                |parsed| Redirect::temporary(parsed.as_str()).into_response(),
            )
        };
        // Take the code Withings sends the browser back with.
        let callback = async |State(app): State<Arc<App>>,
                              Query(query): Query<HashMap<String, String>>|
               -> Response {
            if let Some(error) = query.get("error") {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Withings returned an error: {error}"),
                )
                    .into_response();
            }
            let expected: Option<String> = app.store.get("withings/oauth_state");
            if expected.is_none() || query.get("state") != expected.as_ref() {
                return (
                    StatusCode::BAD_REQUEST,
                    "state mismatch, start again at /withings/auth",
                )
                    .into_response();
            }
            let Some(code) = query.get("code") else {
                return (StatusCode::BAD_REQUEST, "missing code").into_response();
            };
            let Some(client) = Client::new(&app) else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Withings is not configured",
                )
                    .into_response();
            };
            let connected = client.connect(code).await;
            connected.map_or_else(
                |error| {
                    (StatusCode::BAD_GATEWAY, format!("token exchange failed: {error:#}"))
                        .into_response()
                },
                |userid| {
                    format!("Withings connected for user {userid}. Notifications subscribed. The next run will pull all history.\n")
                        .into_response()
                },
            )
        };
        // Withings posts form fields. Answer at once, work in the background.
        let notify = async |State(app): State<Arc<App>>, body: String| -> Response {
            info!("withings: notification {body}");
            let notification = Notification(serde_urlencoded::from_str(&body).unwrap_or_default());
            let tracker = app.tasks.clone();
            drop(tracker.spawn(notification.handle(app, Utc::now())));
            StatusCode::OK.into_response()
        };
        Router::new()
            .route("/withings/auth", get(auth))
            .route("/withings/callback", get(callback))
            .route(
                "/withings/notify",
                post(notify).head(async || StatusCode::OK),
            )
    }
}

/// A fresh 32-hex-digit `OAuth2` state.
pub(super) fn random_state() -> String {
    format!("{:032x}", rand::random::<u128>())
}
