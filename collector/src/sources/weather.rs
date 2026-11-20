//! Fetch weather and air quality from Open-Meteo for the phone's last known location, plus a one-off archive backfill.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Datelike as _, Duration, Months, NaiveDate, Utc};
use futures_util::future::join_all;
use serde_json::{Value, json};
use tracing::info;
use url::Url;

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Hourly air quality variables.
const AIR: &str = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,nitrogen_monoxide,sulphur_dioxide,ozone,ammonia,aerosol_optical_depth,dust,uv_index,uv_index_clear_sky,alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen,european_aqi,european_aqi_pm2_5,european_aqi_pm10,european_aqi_nitrogen_dioxide,european_aqi_ozone,european_aqi_sulphur_dioxide";
/// The air quality endpoint.
const AIR_QUALITY: &str = "https://air-quality-api.open-meteo.com/v1/air-quality";
/// The historical weather endpoint.
const ARCHIVE: &str = "https://archive-api.open-meteo.com/v1/archive";
/// The archive trails real time by a day or two.
const ARCHIVE_LAG_DAYS: i64 = 2;
/// Set this key to a date through `PUT /admin/state` to backfill from that date. It moves forward and is removed when done.
const BACKFILL_KEY: &str = "weather/backfill_from";
/// Months per run. Each month is two requests of about five weighted Open-Meteo calls each.
const BACKFILL_MONTHS: usize = 12;
/// Current weather variables.
const CURRENT: &str = "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m";
/// Daily weather variables.
const DAILY: &str = "weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,sunrise,sunset,daylight_duration,sunshine_duration,uv_index_max,uv_index_clear_sky_max,precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours,precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration,relative_humidity_2m_mean,dew_point_2m_mean,pressure_msl_mean,cloud_cover_mean,visibility_mean";
/// The forecast endpoint.
const FORECAST: &str = "https://api.open-meteo.com/v1/forecast";
/// Hourly weather variables. Also requested from the archive, which returns null for the forecast-only ones.
const HOURLY: &str = "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,vapour_pressure_deficit,wind_speed_10m,wind_direction_10m,wind_gusts_10m,uv_index,uv_index_clear_sky,is_day,sunshine_duration,shortwave_radiation,direct_radiation,diffuse_radiation,wet_bulb_temperature_2m,et0_fao_evapotranspiration,boundary_layer_height,cape,lightning_potential";
/// Quarter-hourly weather variables.
const MINUTELY_15: &str = "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,snowfall,weather_code,visibility,wind_speed_10m,wind_direction_10m,wind_gusts_10m,is_day,sunshine_duration,shortwave_radiation,direct_radiation,diffuse_radiation,lightning_potential";

/// Open-Meteo at the phone's last known location.
struct OpenMeteo<'app> {
    /// The app, for its HTTP client, configuration and store.
    app: &'app App,
    /// The location from the store, with `latitude` and `longitude`.
    location: Value,
}

/// The weather source.
pub(super) struct Weather;

impl OpenMeteo<'_> {
    /// Pull up to `BACKFILL_MONTHS` calendar months of archive weather and air quality, one file each per month.
    async fn backfill(&self, now: DateTime<Utc>) -> Result<()> {
        let store = &self.app.store;
        let Some(mut start) = store.get::<NaiveDate>(BACKFILL_KEY) else {
            return Ok(());
        };
        let end = tri!(
            now.date_naive()
                .checked_sub_signed(Duration::days(ARCHIVE_LAG_DAYS))
                .context("archive end")
        );
        let mut files = Vec::new();
        for _ in 0..BACKFILL_MONTHS {
            if start >= end {
                break;
            }
            let next =
                tri!(start
                .with_day(1)
                .and_then(|first_of_month| first_of_month.checked_add_months(Months::new(1)))
                .context("next month"))
                .min(end);
            let last = tri!(next.pred_opt().context("day before")).to_string();
            let first = start.to_string();
            let at = tri!(start.and_hms_opt(0, 0, 0).context("midnight")).and_utc();
            let weather = [
                ("start_date", first.as_str()),
                ("end_date", last.as_str()),
                ("hourly", HOURLY),
                ("daily", DAILY),
            ];
            let air = [
                ("start_date", first.as_str()),
                ("end_date", last.as_str()),
                ("hourly", AIR),
            ];
            files.push(tri!(
                self.fetch(ARCHIVE, "open_meteo_archive", at, &weather)
                    .await
            ));
            files.push(tri!(
                self.fetch(AIR_QUALITY, "open_meteo_air_quality", at, &air)
                    .await
            ));
            start = next;
        }
        if !files.is_empty() {
            let message = format!("weather: backfill to {start}");
            drop(tri!(self.app.save(&files, &message).await));
        }
        if start >= end {
            info!("weather: backfill done");
            store.remove(BACKFILL_KEY)
        } else {
            store.set(BACKFILL_KEY, &start)
        }
    }

    /// GET one endpoint, in UTC, and wrap the reply as a file of `kind` named by `at`.
    async fn fetch(
        &self,
        endpoint: &str,
        kind: &str,
        at: DateTime<Utc>,
        params: &[(&str, &str)],
    ) -> Result<File> {
        let lat = tri!(
            self.location
                .get("latitude")
                .and_then(Value::as_f64)
                .context("location latitude")
        );
        let lon = tri!(
            self.location
                .get("longitude")
                .and_then(Value::as_f64)
                .context("location longitude")
        );
        let base = self.app.cfg.open_meteo_url.as_deref().map_or_else(
            || endpoint.to_owned(),
            |test_url| endpoint.replacen("https://", &format!("{test_url}/"), 1),
        );
        let latitude = format!("{lat:.4}");
        let longitude = format!("{lon:.4}");
        let query: Vec<(&str, &str)> = [
            ("latitude", latitude.as_str()),
            ("longitude", longitude.as_str()),
        ]
        .into_iter()
        .chain(params.iter().copied())
        .chain([("timezone", "UTC")])
        .collect();
        let url = tri!(Url::parse_with_params(&base, &query));
        let resp = tri!(self.app.http.get(url.clone()).send().await);
        let status = resp.status();
        let text = tri!(resp.text().await);
        if !status.is_success() {
            bail!("open-meteo {kind} {status}: {text}");
        }
        Ok(File {
            path: storage::path("weather", kind, at, None, "json"),
            content: tri!(storage::envelope(
                Utc::now(),
                json!({"url": url.as_str(), "location": self.location}),
                &text,
            )),
        })
    }
}

#[async_trait]
impl Source for Weather {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(location) = app.store.get::<Value>("location") else {
            return Ok(());
        };
        let meteo = OpenMeteo { app, location };
        let file = tri!(
            meteo
                .fetch(
                    FORECAST,
                    "open_meteo_daily",
                    Utc::now(),
                    &[("daily", DAILY), ("past_days", "2"), ("forecast_days", "1")],
                )
                .await
        );
        drop(tri!(app.save(&[file], "weather: open_meteo_daily").await));
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(location) = app.store.get::<Value>("location") else {
            info!("weather: no location yet, skipping");
            return Ok(());
        };
        let meteo = OpenMeteo { app, location };
        let now = Utc::now();
        let results = join_all([
            meteo.fetch(
                FORECAST,
                "open_meteo",
                now,
                &[
                    ("current", CURRENT),
                    ("hourly", HOURLY),
                    ("minutely_15", MINUTELY_15),
                    ("past_hours", "3"),
                    ("forecast_hours", "1"),
                    ("past_minutely_15", "8"),
                    ("forecast_minutely_15", "0"),
                ],
            ),
            meteo.fetch(
                AIR_QUALITY,
                "open_meteo_air_quality",
                now,
                &[
                    ("hourly", AIR),
                    ("past_hours", "3"),
                    ("forecast_hours", "1"),
                ],
            ),
        ])
        .await;
        // Commit whatever arrived, so one failing API does not lose the other's data.
        let mut files = Vec::new();
        let mut failure = None;
        for result in results {
            match result {
                Ok(file) => files.push(file),
                Err(error) => failure = failure.or(Some(error)),
            }
        }
        if !files.is_empty() {
            let kinds: Vec<&str> = files
                .iter()
                .filter_map(|file| file.path.split('/').nth(2))
                .collect();
            let message = format!("weather: {}", kinds.join(", "));
            drop(tri!(app.save(&files, &message).await));
        }
        if let Some(error) = failure {
            return Err(error);
        }
        tri!(meteo.backfill(now).await);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "weather"
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}
