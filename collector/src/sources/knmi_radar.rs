//! Rain at home every 5 minutes from KNMI's precipitation radar, through the KNMI Data Platform WMS (key
//! `KNMI_WMS_KEY`).
//!
//! The `radar_forecast` dataset holds a nowcast run every 5 minutes, forecasting 2 hours ahead; the step of a run at
//! its own reference time is the radar analysis, i.e. measured rain rather than a forecast. The server keeps 7 days and
//! refuses requests over 512 values, so each hourly run asks one nowcast run at a time (all 25 steps) with
//! `GetFeatureInfo` at home, from the last stored run on, at most 96 runs (8 hours) per hourly run. The replies are
//! committed together as a JSON list, verbatim.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::ops::{Add as _, Sub as _};

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, DurationRound as _, SecondsFormat, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};
use url::Url;

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Store key of the next step to ask for.
const CURSOR: &str = "knmi_radar/next";
/// Half the side of the query box around home, in degrees; the radar grid is about 1 km.
const HALF_BOX: f64 = 0.0005;
/// How far ahead a nowcast run forecasts.
const HORIZON: Duration = Duration::hours(2);
/// The server keeps a week of runs.
const KEPT: Duration = Duration::days(7);
/// A step still missing this long after its time is a gap, not a run still to come.
const MISSING_AFTER: Duration = Duration::minutes(30);
/// Directory name under `raw/`.
const SOURCE: &str = "knmi_radar";
/// The interval between nowcast runs.
const STEP: Duration = Duration::minutes(5);
/// Runs to ask for per hourly run.
const STEPS_PER_RUN: usize = 96;

/// The KNMI precipitation radar source.
pub(super) struct KnmiRadar;

#[async_trait]
impl Source for KnmiRadar {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let (Some(key), Some((lat, lon))) = (app.cfg.knmi_wms_key.as_deref(), app.cfg.home) else {
            return Ok(());
        };
        let now = Utc::now();
        let week_ago = tri!(
            now.checked_sub_signed(KEPT)
                .and_then(|kept| kept.checked_add_signed(Duration::hours(1)))
                .context("oldest kept run")
        );
        let oldest = tri!(week_ago.duration_trunc(STEP));
        let mut step = app
            .store
            .get::<DateTime<Utc>>(CURSOR)
            .unwrap_or(oldest)
            .max(oldest);
        let bbox = format!(
            "{:.4},{:.4},{:.4},{:.4}",
            lat.sub(HALF_BOX),
            lon.sub(HALF_BOX),
            lat.add(HALF_BOX),
            lon.add(HALF_BOX)
        );
        let endpoint = format!("{}/wms/adaguc-server", app.cfg.knmi_wms_url);
        let mut replies = Vec::new();
        while replies.len() < STEPS_PER_RUN && step < now {
            let at = step.to_rfc3339_opts(SecondsFormat::Secs, true);
            let horizon = tri!(step.checked_add_signed(HORIZON).context("forecast horizon"));
            let span = format!(
                "{at}/{}",
                horizon.to_rfc3339_opts(SecondsFormat::Secs, true)
            );
            let url = tri!(Url::parse_with_params(
                &endpoint,
                [
                    ("DATASET", "radar_forecast"),
                    ("SERVICE", "WMS"),
                    ("VERSION", "1.3.0"),
                    ("REQUEST", "GetFeatureInfo"),
                    ("LAYERS", "precipitationfc"),
                    ("QUERY_LAYERS", "precipitationfc"),
                    ("CRS", "EPSG:4326"),
                    ("BBOX", bbox.as_str()),
                    ("WIDTH", "3"),
                    ("HEIGHT", "3"),
                    ("I", "1"),
                    ("J", "1"),
                    ("INFO_FORMAT", "application/json"),
                    ("TIME", span.as_str()),
                    ("DIM_REFERENCE_TIME", at.as_str()),
                ],
            ));
            let resp = tri!(app.http.get(url).header("Authorization", key).send().await);
            let status = resp.status();
            let text = tri!(resp.text().await);
            let next = tri!(step.checked_add_signed(STEP).context("next step"));
            // No run for this step (404 InvalidDimensionValue): not published yet, so ask again next hour; or, when
            // older than half an hour, a gap in the radar, so skip it rather than wait for ever.
            if text.contains("ServiceException") {
                if status == StatusCode::NOT_FOUND || text.contains("InvalidDimensionValue") {
                    if now.signed_duration_since(step) < MISSING_AFTER {
                        break;
                    }
                    step = next;
                    continue;
                }
                bail!(
                    "knmi radar {at} {status}: {}",
                    text.chars().take(300).collect::<String>()
                );
            }
            if !status.is_success() {
                bail!("knmi radar {at} {status}");
            }
            let reply: Value = tri!(
                serde_json::from_str(&text).with_context(|| format!("knmi radar {at}: not JSON"))
            );
            replies.push(json!({"time": at, "reply": reply}));
            step = next;
        }
        if replies.is_empty() {
            return Ok(());
        }
        let file = File {
            path: storage::path(SOURCE, "precipitation", now, None, "json"),
            content: tri!(serde_json::to_string(&replies)),
        };
        drop(tri!(
            app.save(&[file], &format!("knmi_radar: {} steps", replies.len()))
                .await
        ));
        tri!(app.store.set(CURSOR, &step));
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}
