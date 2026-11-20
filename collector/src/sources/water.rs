//! Water temperatures around Amsterdam and the coast from Rijkswaterstaat, and the status of every official swimming
//! spot from zwemwater.nl, to tell a good day for a swim.
//!
//! Hourly, each Rijkswaterstaat station that measures every 10 minutes is asked for its measurements since the last
//! one stored, so nothing is stored twice; the reply is committed verbatim. Stations sampled by hand about monthly are
//! asked daily for the last 45 days, since samples are entered late. Daily, the swimming spot list embedded in the
//! zwemwater.nl home page is committed when it changed; zwemwater.nl has no API, so this can break.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, FixedOffset, SecondsFormat, TimeDelta, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Directory name under `raw/`.
const SOURCE: &str = "water";
/// Stations measuring water temperature every 10 minutes: the IJ, the Amsterdam-Rijnkanaal, the North Sea Canal at
/// Spaarndam, the sea side of `IJmuiden` and the Markermeer.
const LIVE: [&str; 6] = [
    "amsterdam.ndsmwerf",
    "amsterdam.schellingwouderbrug",
    "diemen",
    "spaarndam.zijkanaalc",
    "ijmuiden.buitenhaven",
    "markermeer.midden",
];
/// Stations with only hand samples, about monthly.
const SAMPLED: [&str; 2] = ["amsterdam.km25", "pampus.oost"];
/// How far back a station without a cursor is read, and the longest window asked for at once.
const FIRST_WINDOW: TimeDelta = TimeDelta::days(7);
/// How far back the sampled stations are read, daily.
const SAMPLE_WINDOW: TimeDelta = TimeDelta::days(45);
/// Rijkswaterstaat writes every time with a fixed +01:00 offset, also in summer.
const RWS_OFFSET: FixedOffset = FixedOffset::east_opt(3600).unwrap();
/// Store key of the hash of the last committed swimming spot list.
const SPOTS_SHA: &str = "water/sha/swimming_spots";

/// The water source.
pub(super) struct Water;

#[async_trait]
impl Source for Water {
    async fn daily(&self, app: &App) -> Result<()> {
        if app.cfg.rws_url.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let since = tri!(
            now.checked_sub_signed(SAMPLE_WINDOW)
                .context("sample window start")
        );
        let mut files = Vec::new();
        for station in SAMPLED {
            if let Some((file, _)) = tri!(fetch(app, station, since, now).await) {
                files.push(file);
            }
        }
        let page = tri!(
            app.http
                .get(&app.cfg.zwemwater_url)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await
        );
        let html = tri!(tri!(page.error_for_status()).text().await);
        // The JSON list in the `data-swimming-spots` attribute of the home page, HTML entities decoded.
        let spots = tri!(html
            .split_once("data-swimming-spots='")
            .and_then(|(_, rest)| rest.split_once('\''))
            .map(|(raw, _)| {
                raw.replace("&quot;", "\"")
                    .replace("&#039;", "'")
                    .replace("&apos;", "'")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&amp;", "&")
            })
            .filter(|text| serde_json::from_str::<Value>(text).is_ok_and(|list| list.is_array()))
            .context("zwemwater.nl: no swimming spot list in the home page"));
        let sha = steam::changed(app, SPOTS_SHA, &spots);
        if sha.is_some() {
            files.push(File {
                path: storage::path(SOURCE, "swimming_spots", now, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    json!({"url": app.cfg.zwemwater_url}),
                    &spots
                )),
            });
        }
        drop(tri!(
            app.save(&files, &format!("water: daily ({} files)", files.len()))
                .await
        ));
        if let Some(spots_sha) = sha {
            tri!(app.store.set(SPOTS_SHA, &spots_sha));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        if app.cfg.rws_url.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let window_start = tri!(now.checked_sub_signed(FIRST_WINDOW).context("window start"));
        let mut files = Vec::new();
        let mut cursors = Vec::new();
        for station in LIVE {
            let key = format!("water/cursor/{station}");
            let since = app
                .store
                .get::<DateTime<Utc>>(&key)
                .unwrap_or(window_start)
                .max(window_start);
            let Some((file, newest)) = tri!(fetch(app, station, since, now).await) else {
                continue;
            };
            files.push(file);
            if let Some(time) = newest {
                cursors.push((key, time));
            }
        }
        drop(tri!(
            app.save(
                &files,
                &format!("water: temperature ({} stations)", files.len()),
            )
            .await
        ));
        for (key, time) in cursors {
            tri!(app.store.set(&key, &time));
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

/// The water temperature measurements of `station` from `since` to `until`, as a file, and the newest measurement
/// time; None when there are none.
async fn fetch(
    app: &App,
    station: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<Option<(File, Option<DateTime<Utc>>)>> {
    let body = json!({
        "Locatie": {"Code": station},
        "AquoPlusWaarnemingMetadata": {"AquoMetadata": {"Compartiment": {"Code": "OW"}, "Grootheid": {"Code": "T"}}},
        "Periode": {"Begindatumtijd": rws_time(since), "Einddatumtijd": rws_time(until)},
    });
    let url = format!(
        "{}/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        app.cfg.rws_url
    );
    let resp = tri!(app.http.post(&url).json(&body).send().await);
    let status = resp.status();
    if status == StatusCode::NO_CONTENT {
        return Ok(None);
    }
    let text = tri!(resp.text().await);
    if !status.is_success() {
        let snippet: String = text.chars().take(300).collect();
        bail!("rijkswaterstaat {station}: {status} {snippet}");
    }
    let reply: Value = tri!(serde_json::from_str(&text));
    let newest = reply
        .get("WaarnemingenLijst")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|observation| observation.get("MetingenLijst"))
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(|measurement| measurement.get("Tijdstip"))
        .filter_map(Value::as_str)
        .filter_map(|time| DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.with_timezone(&Utc))
        .max();
    let file = File {
        path: storage::path(SOURCE, "temperature", until, Some(station), "json"),
        content: tri!(storage::envelope(
            Utc::now(),
            json!({"url": url, "body": body}),
            &text
        )),
    };
    Ok(Some((file, newest)))
}

/// Format a time as Rijkswaterstaat writes it, with milliseconds at +01:00.
fn rws_time(time: DateTime<Utc>) -> String {
    time.with_timezone(&RWS_OFFSET)
        .to_rfc3339_opts(SecondsFormat::Millis, false)
}
