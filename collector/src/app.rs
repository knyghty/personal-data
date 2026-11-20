//! Process-wide configuration and shared clients, the routes and the job run.

use alloc::sync::Arc;
use core::str::FromStr;
use core::time::Duration;
use std::collections::HashMap;
use std::env::Vars;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context as _, Result};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as UrlPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::future::join_all;
use reqwest::Client;
use rusty_s3::{Bucket, Credentials, UrlStyle};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::github::GitHub;
use crate::monitor::{Cron, Routine};
use crate::raw::Raw;
use crate::sources::{self, DegiroLogin};
use crate::storage::File;
use crate::store::Store;
use crate::tri;

/// Consecutive failed runs of a job before it gets a Sentry issue and a failed check-in. Upstream APIs time out or
/// answer 5xx for an hour now and then, and the next run picks up where the cursor stayed.
const ISSUE_AFTER: u32 = 2;

/// Shared state of every request handler and job.
pub(crate) struct App {
    /// Settings from the environment.
    pub cfg: Config,
    /// Sentry cron check-ins.
    pub cron: Cron,
    /// The data repo.
    pub github: GitHub,
    /// Shared HTTP client.
    pub http: Client,
    /// Held while a job run is in progress, so runs are single-flight.
    pub job_lock: Mutex<()>,
    /// The bucket that holds `raw/`.
    pub raw: Raw,
    /// The fixer routine.
    pub routine: Routine,
    /// Cursors and tokens.
    pub store: Store,
    /// Background work spawned by request handlers; awaited on shutdown.
    pub tasks: TaskTracker,
}

/// Settings from the environment. A `*_url` field that tests point at a fake has no other reason to exist.
pub(crate) struct Config {
    /// Bearer token of the admin endpoints.
    pub admin_secret: String,
    /// Origin of the Albert Heijn mobile API.
    pub albert_heijn_url: String,
    /// Bearer token the phone sends with Apple Health exports.
    pub apple_health_secret: Option<String>,
    /// Audible API base instead of `https://api.audible.<domain>`.
    pub audible_api_url: Option<String>,
    /// API key of the personal bunq account.
    pub bunq_api_key: Option<String>,
    /// Origin of the bunq API, without `/v1`. Point it at the sandbox to test.
    pub bunq_api_url: String,
    /// API key of the business bunq account.
    pub bunq_business_api_key: Option<String>,
    /// JSON object of calendar name to `{"url", "redact"}`. Kept unparsed so a bad value fails the source, not the start.
    pub calendars: Option<String>,
    /// DEGIRO web trader login of the personal account. Without one, that source is off.
    pub degiro: Option<DegiroLogin>,
    /// DEGIRO web trader login of the pension account. Without one, that source is off.
    pub degiro_pension: Option<DegiroLogin>,
    /// Origin of the DEGIRO web trader.
    pub degiro_url: String,
    /// KNMI's FDSN seismology service. Empty turns the source off.
    pub fdsn_url: String,
    /// Token for the GitHub activity source.
    pub github_activity_token: Option<String>,
    /// GitHub API base, shared by the data repo client and the activity source.
    pub github_api: String,
    /// GitHub user whose activity to pull.
    pub github_user: Option<String>,
    /// The Gmail API base.
    pub gmail_api_url: String,
    /// Senders whose mail is stored whole, as addresses or domains; everyone else's is metadata only.
    pub gmail_full_senders: Vec<String>,
    /// OAuth client id of the Google Cloud app for the Data Portability API.
    pub google_client_id: Option<String>,
    /// OAuth client secret of the Google Cloud app.
    pub google_client_secret: Option<String>,
    /// The Data Portability API base.
    pub google_portability_url: String,
    /// Google's OAuth token endpoint.
    pub google_token_url: String,
    /// Days of Home Assistant history to backfill.
    pub ha_backfill_days: i64,
    /// Entity that tracks the phone's location.
    pub ha_location_entity: Option<String>,
    /// Home Assistant notify service that gets the offer alerts, e.g. `mobile_app_toms_iphone`. Unset sends none.
    pub ha_notify: Option<String>,
    /// Days of Home Assistant long-term statistics to backfill.
    pub ha_stats_backfill_days: i64,
    /// Home Assistant long-lived access token.
    pub ha_token: Option<String>,
    /// Home Assistant origin.
    pub ha_url: Option<String>,
    /// Home as (lat, lon), for data asked for at a point.
    pub home: Option<(f64, f64)>,
    /// Kindle Whispersync host.
    pub kindle_api_url: String,
    /// Kindle annotation (sidecar) host.
    pub kindle_cde_url: String,
    /// Kindle library sync host.
    pub kindle_todo_url: String,
    /// KNMI Data Platform EDR API key. Without a key the source is off.
    pub knmi_edr_key: Option<String>,
    /// KNMI Data Platform EDR API base.
    pub knmi_edr_url: String,
    /// KNMI's climatology service. Empty turns the source off.
    pub knmi_url: String,
    /// KNMI Data Platform WMS key, for the rain radar. Without a key the source is off.
    pub knmi_wms_key: Option<String>,
    /// KNMI Data Platform WMS base.
    pub knmi_wms_url: String,
    /// Last.fm API key.
    pub lastfm_api_key: Option<String>,
    /// Last.fm API endpoint.
    pub lastfm_api_url: String,
    /// Last.fm user whose scrobbles to pull.
    pub lastfm_user: Option<String>,
    /// Luchtmeetnet station numbers to pull. Empty turns the source off.
    pub luchtmeetnet_stations: Vec<String>,
    /// Base of the Luchtmeetnet open API.
    pub luchtmeetnet_url: String,
    /// Moneybird API base.
    pub moneybird_api_url: String,
    /// Moneybird personal access token. Without a token the source is off.
    pub moneybird_token: Option<String>,
    /// Bearer token the Home Assistant Pi sends with network pushes.
    pub network_secret: Option<String>,
    /// `PrijsProfeet` retailer keys whose offers to pull, e.g. `albert_heijn`. Empty pulls none.
    pub offers_prijsprofeet: Vec<String>,
    /// Prefix for every Open-Meteo URL, which then reads `<prefix>/<host>/<path>`. Tests point it at a fake.
    pub open_meteo_url: Option<String>,
    /// `PrijsProfeet` API key, which raises the rate limit. Without it the public limit applies.
    pub prijsprofeet_key: Option<String>,
    /// `PrijsProfeet` API base.
    pub prijsprofeet_url: String,
    /// Where the outside world reaches this server, for OAuth redirects.
    pub public_url: String,
    /// Rijkswaterstaat's water data API. Empty turns the water source off.
    pub rws_url: String,
    /// Bearer token the Mac sends with Screen Time exports.
    pub screen_time_secret: Option<String>,
    /// Steam Web API key.
    pub steam_api_key: Option<String>,
    /// Origin of the Steam Web API.
    pub steam_api_url: String,
    /// 64-bit `SteamID` of the account to pull.
    pub steam_id: Option<String>,
    /// Twitch's Helix API base.
    pub twitch_api_url: String,
    /// Twitch application client id.
    pub twitch_client_id: Option<String>,
    /// Twitch application client secret.
    pub twitch_client_secret: Option<String>,
    /// Twitch's OAuth base.
    pub twitch_id_url: String,
    /// Earliest day to backfill Withings data from.
    pub withings_backfill_from: Option<String>,
    /// Withings application client id.
    pub withings_client_id: Option<String>,
    /// Withings application client secret.
    pub withings_client_secret: Option<String>,
    /// `YouTube` Data API key.
    pub youtube_api_key: Option<String>,
    /// Base of the `YouTube` Data API.
    pub youtube_api_url: String,
    /// The zwemwater.nl home page.
    pub zwemwater_url: String,
}

/// One failed job of a run.
#[derive(PartialEq, Eq)]
pub(crate) struct Failure {
    /// The error, without the `error: ` prefix of the report.
    pub error: String,
    /// `hourly` or `daily`.
    pub job: String,
    /// The source's name.
    pub source: String,
}

/// What one run did, as the admin endpoint returns it.
#[derive(Serialize)]
pub(crate) struct Report {
    /// Result per source of the daily jobs that were due: `ok` or `error: <message>`.
    pub daily: Map<String, Value>,
    /// Result per source of the hourly jobs.
    pub hourly: Map<String, Value>,
    /// Wall-clock seconds the run took.
    pub seconds: f64,
}

impl App {
    /// Whether the request carries the admin secret.
    pub(crate) fn admin_ok(&self, headers: &HeaderMap) -> bool {
        Self::bearer(headers) == Some(self.cfg.admin_secret.as_str())
    }

    /// The bearer token of a request, if any.
    pub(crate) fn bearer(headers: &HeaderMap) -> Option<&str> {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|text| text.strip_prefix("Bearer "))
            .map(str::trim)
    }

    /// Run one source's daily job and, when it succeeds, remember `now` as its last run.
    async fn daily(&self, source: &dyn sources::Source, now: DateTime<Utc>) -> (String, Value) {
        let result = source.daily(self).await;
        if result.is_ok() {
            let stored = self
                .store
                .set(&format!("last_daily/{}", source.name()), &now);
            if let Err(error) = stored {
                error!("store last_daily for {}: {error:#}", source.name());
            }
        }
        (
            source.name().to_owned(),
            outcome(source.name(), "daily", result),
        )
    }

    /// Report a failure that has lasted `ISSUE_AFTER` runs to Sentry, as one issue per source and job, and start the
    /// fixer. Later runs of the same streak add events to that issue but do not start the fixer again.
    async fn escalate(&self, failure: &Failure, count: u32) {
        let (source, job, message) = (&failure.source, &failure.job, &failure.error);
        let title = format!("Collector: {source} {job} failed");
        let event = format!("{title} for {count} runs in a row: {message}");
        let _id = sentry::with_scope(
            |scope| scope.set_fingerprint(Some(&["collector", source.as_str(), job.as_str()])),
            || sentry::capture_message(&event, sentry::Level::Error),
        );
        if count == ISSUE_AFTER && self.routine.configured() {
            let fired = self.routine.fire(&format!("{title}\n\n{message}")).await;
            if let Err(error) = fired {
                error!("fire routine for {source} {job}: {error:#}");
            }
        }
    }

    /// Count the consecutive failures of each job in the store, forget the jobs that succeeded, and return the failures
    /// that have lasted `ISSUE_AFTER` runs, each with its count.
    fn persistent(&self, report: &Report) -> Vec<(Failure, u32)> {
        for (job, results) in report.jobs() {
            for (source, result) in results {
                if result == "ok" {
                    let forgotten = self.store.remove(&format!("failures/{source}/{job}"));
                    if let Err(error) = forgotten {
                        error!("forget failures of {source} {job}: {error:#}");
                    }
                }
            }
        }
        report
            .failures()
            .into_iter()
            .filter_map(|failure| {
                let key = format!("failures/{}/{}", failure.source, failure.job);
                let count = self.store.get::<u32>(&key).unwrap_or(0).saturating_add(1);
                if let Err(error) = self.store.set(&key, &count) {
                    error!("count failures of {key}: {error:#}");
                }
                if count < ISSUE_AFTER {
                    warn!("{key} failed once, waiting for the next run before reporting it");
                }
                (count >= ISSUE_AFTER).then_some((failure, count))
            })
            .collect()
    }

    /// All routes: the admin endpoints plus every source's own.
    pub(crate) fn router(self: Arc<Self>) -> Router {
        let mut router = Router::new()
            .route("/health", get(async || "ok"))
            .route(
                "/admin/run",
                post(
                    async |State(app): State<Arc<Self>>,
                           headers: HeaderMap,
                           Query(query): Query<HashMap<String, String>>| {
                        if !app.admin_ok(&headers) {
                            return StatusCode::UNAUTHORIZED.into_response();
                        }
                        let Ok(guard) = app.job_lock.try_lock() else {
                            return (StatusCode::CONFLICT, "a run is already in progress")
                                .into_response();
                        };
                        let force_daily = query.get("job").map(String::as_str) == Some("daily");
                        let report = app.run(force_daily).await;
                        drop(guard);
                        let status = if report.failures().is_empty() {
                            StatusCode::OK
                        } else {
                            StatusCode::INTERNAL_SERVER_ERROR
                        };
                        (status, Json(report)).into_response()
                    },
                ),
            )
            .route(
                // Store the body as the file at `raw/...`, for files added by hand.
                "/admin/raw/{*path}",
                put(
                    async |State(app): State<Arc<Self>>,
                           headers: HeaderMap,
                           UrlPath(path): UrlPath<String>,
                           body: Bytes| {
                        if !app.admin_ok(&headers) {
                            return StatusCode::UNAUTHORIZED.into_response();
                        }
                        match app.raw.upload(&format!("raw/{path}"), &body).await {
                            Ok(key) => Json(serde_json::json!({"key": key})).into_response(),
                            Err(error) => {
                                (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response()
                            }
                        }
                    },
                )
                .layer(DefaultBodyLimit::max(256 << 20)),
            )
            .route(
                "/admin/state",
                get(async |State(app): State<Arc<Self>>, headers: HeaderMap| {
                    if !app.admin_ok(&headers) {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    Json(app.store.redacted()).into_response()
                })
                .put(
                    // Merge the JSON object in the body into the store, for example to move a cursor. A null value
                    // removes the key.
                    async |State(app): State<Arc<Self>>,
                           headers: HeaderMap,
                           Json(body): Json<Map<String, Value>>|
                           -> Response {
                        if !app.admin_ok(&headers) {
                            return StatusCode::UNAUTHORIZED.into_response();
                        }
                        for (key, value) in body {
                            let result = if value.is_null() {
                                app.store.remove(&key)
                            } else {
                                app.store.set(&key, &value)
                            };
                            if let Err(error) = result {
                                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"))
                                    .into_response();
                            }
                        }
                        Json(app.store.redacted()).into_response()
                    },
                ),
            );
        for source in sources::all() {
            router = router.merge(source.router());
        }
        router.with_state(self)
    }

    /// Run the due jobs, count the failures and escalate the persistent ones.
    pub(crate) async fn run(&self, force_daily: bool) -> Report {
        let check_in = self.cron.start(&self.http).await;
        let report = self.run_jobs(force_daily).await;
        let persistent = self.persistent(&report);
        self.cron
            .finish(&self.http, &check_in, persistent.is_empty())
            .await;
        for streak in &persistent {
            self.escalate(&streak.0, streak.1).await;
        }
        report
    }

    /// Run every source's hourly job concurrently, then the daily jobs that are due.
    async fn run_jobs(&self, force_daily: bool) -> Report {
        let started = Instant::now();
        let all = sources::all();
        let hourly = join_all(all.iter().map(async move |source| {
            let result = source.hourly(self).await;
            (
                source.name().to_owned(),
                outcome(source.name(), "hourly", result),
            )
        }))
        .await;
        let now = Utc::now();
        let due: Vec<_> = all
            .iter()
            .filter(|source| {
                let last: Option<DateTime<Utc>> =
                    self.store.get(&format!("last_daily/{}", source.name()));
                force_daily
                    || last.is_none_or(|at| now.signed_duration_since(at) > TimeDelta::hours(23))
            })
            .collect();
        let daily = join_all(due.iter().map(|source| self.daily(source.as_ref(), now))).await;
        Report {
            daily: daily.into_iter().collect(),
            hourly: hourly.into_iter().collect(),
            seconds: started.elapsed().as_secs_f64(),
        }
    }

    /// Store `files` in the bucket and tell the query server. Returns the first file's key, or `None` when there is
    /// nothing to store.
    ///
    /// # Errors
    /// An upload to the bucket fails for good.
    pub(crate) async fn save(&self, files: &[File], message: &str) -> Result<Option<String>> {
        self.raw.save(files, message).await
    }
}

impl Report {
    /// The failed jobs, in report order.
    pub(crate) fn failures(&self) -> Vec<Failure> {
        let mut out = Vec::new();
        for (job, results) in self.jobs() {
            for (source, result) in results {
                if let Some(error) = result
                    .as_str()
                    .and_then(|text| text.strip_prefix("error: "))
                {
                    out.push(Failure {
                        error: error.to_owned(),
                        job: job.to_owned(),
                        source: source.clone(),
                    });
                }
            }
        }
        out
    }

    /// Each job's results, hourly first.
    const fn jobs(&self) -> [(&'static str, &Map<String, Value>); 2] {
        [("hourly", &self.hourly), ("daily", &self.daily)]
    }
}

/// The process environment, read once.
pub(crate) struct Env(HashMap<String, String>);

impl Env {
    /// Build the shared state from the settings.
    ///
    /// # Errors
    /// A required variable is unset, or the state file cannot be read.
    pub(crate) fn app(&self) -> Result<App> {
        let http = tri!(
            Client::builder()
                .user_agent("personal-data-collector")
                .timeout(Duration::from_mins(3))
                .build()
        );
        let github_api = self.url("GITHUB_API", "https://api.github.com");
        let github = tri!(self.github(http.clone(), &github_api));
        let state_path = self
            .get("STATE_PATH")
            .unwrap_or_else(|| "/data/state.json".into());
        let cfg = Config {
            admin_secret: tri!(self.required("ADMIN_SECRET")),
            albert_heijn_url: "https://api.ah.nl".into(),
            apple_health_secret: self.get("APPLE_HEALTH_SECRET"),
            audible_api_url: None,
            bunq_api_key: self.get("BUNQ_API_KEY"),
            bunq_api_url: self.url("BUNQ_API_URL", "https://api.bunq.com"),
            bunq_business_api_key: self.get("BUNQ_BUSINESS_API_KEY"),
            calendars: self.get("CALENDARS"),
            degiro: self.login("DEGIRO"),
            degiro_pension: self.login("DEGIRO_PENSION"),
            degiro_url: "https://trader.degiro.nl".into(),
            fdsn_url: "https://rdsa.knmi.nl".into(),
            github_activity_token: self.get("GITHUB_ACTIVITY_TOKEN"),
            github_api,
            github_user: self.get("GITHUB_USER"),
            gmail_api_url: "https://gmail.googleapis.com/gmail/v1".into(),
            gmail_full_senders: self
                .list("GMAIL_FULL_SENDERS", "")
                .iter()
                .map(|sender| sender.to_ascii_lowercase())
                .collect(),
            google_client_id: self.get("GOOGLE_CLIENT_ID"),
            google_client_secret: self.get("GOOGLE_CLIENT_SECRET"),
            google_portability_url: "https://dataportability.googleapis.com".into(),
            google_token_url: "https://oauth2.googleapis.com/token".into(),
            ha_backfill_days: self.parsed("HA_BACKFILL_DAYS").unwrap_or(10),
            ha_location_entity: self.get("HA_LOCATION_ENTITY"),
            ha_notify: self.get("HA_NOTIFY"),
            ha_stats_backfill_days: self.parsed("HA_STATS_BACKFILL_DAYS").unwrap_or(365),
            ha_token: self.get("HA_TOKEN"),
            ha_url: self
                .get("HA_URL")
                .map(|url| url.trim_end_matches('/').to_owned()),
            home: self.parsed("HOME_LAT").zip(self.parsed("HOME_LON")),
            kindle_api_url: "https://api.amazon.com".into(),
            kindle_cde_url: "https://cde-ta-g7g.amazon.com".into(),
            kindle_todo_url: "https://todo-ta-g7g.amazon.com".into(),
            knmi_edr_key: self.get("KNMI_EDR_KEY"),
            knmi_edr_url: "https://api.dataplatform.knmi.nl/edr/v1".into(),
            knmi_url: "https://www.daggegevens.knmi.nl".into(),
            knmi_wms_key: self.get("KNMI_WMS_KEY"),
            knmi_wms_url: "https://api.dataplatform.knmi.nl".into(),
            lastfm_api_key: self.get("LASTFM_API_KEY"),
            lastfm_api_url: "https://ws.audioscrobbler.com/2.0/".into(),
            lastfm_user: self.get("LASTFM_USER"),
            // NL49017 Amsterdam-Stadhouderskade is about 3.5 km from home.
            luchtmeetnet_stations: self.list("LUCHTMEETNET_STATIONS", "NL49017"),
            luchtmeetnet_url: "https://api.luchtmeetnet.nl/open_api".into(),
            moneybird_api_url: "https://moneybird.com/api/v2".into(),
            moneybird_token: self.get("MONEYBIRD_TOKEN"),
            network_secret: self.get("NETWORK_SECRET"),
            offers_prijsprofeet: self.list("OFFERS_PRIJSPROFEET", "albert_heijn,dirk"),
            open_meteo_url: None,
            prijsprofeet_key: self.get("PRIJSPROFEET_KEY"),
            prijsprofeet_url: "https://www.prijsprofeet.nl/api/v1".into(),
            public_url: tri!(self.required("PUBLIC_URL"))
                .trim_end_matches('/')
                .to_owned(),
            rws_url: "https://ddapi20-waterwebservices.rijkswaterstaat.nl".into(),
            screen_time_secret: self.get("SCREEN_TIME_SECRET"),
            steam_api_key: self.get("STEAM_API_KEY"),
            steam_api_url: self.url("STEAM_API_URL", "https://api.steampowered.com"),
            steam_id: self.get("STEAM_ID"),
            twitch_api_url: "https://api.twitch.tv/helix".into(),
            twitch_client_id: self.get("TWITCH_CLIENT_ID"),
            twitch_client_secret: self.get("TWITCH_CLIENT_SECRET"),
            twitch_id_url: "https://id.twitch.tv".into(),
            withings_backfill_from: self.get("WITHINGS_BACKFILL_FROM"),
            withings_client_id: self.get("WITHINGS_CLIENT_ID"),
            withings_client_secret: self.get("WITHINGS_CLIENT_SECRET"),
            youtube_api_key: self.get("YOUTUBE_API_KEY"),
            youtube_api_url: "https://www.googleapis.com/youtube/v3".into(),
            zwemwater_url: "https://www.zwemwater.nl/".into(),
        };
        Ok(App {
            cfg,
            cron: self.cron(),
            github,
            http: http.clone(),
            job_lock: Mutex::new(()),
            raw: tri!(self.raw(http.clone())),
            routine: self.routine(http),
            store: tri!(Store::try_from(Path::new(&state_path))),
            tasks: TaskTracker::new(),
        })
    }

    /// Check-ins for `SENTRY_DSN`, or none; a bad DSN turns them off.
    fn cron(&self) -> Cron {
        self.get("SENTRY_DSN").map_or_else(Cron::default, |dsn| {
            dsn.parse().unwrap_or_else(|error| {
                warn!("sentry: {error:#}, cron monitoring disabled");
                Cron::default()
            })
        })
    }

    /// A non-empty variable.
    fn get(&self, key: &str) -> Option<String> {
        self.0
            .get(key)
            .filter(|value| !value.trim().is_empty())
            .cloned()
    }

    /// The code repo client on `api`. A public repo needs no `GITHUB_TOKEN`.
    fn github(&self, http: Client, api: &str) -> Result<GitHub> {
        Ok(GitHub {
            api: api.to_owned(),
            branch: self.get("GITHUB_BRANCH").unwrap_or_else(|| "main".into()),
            http,
            repo: tri!(self.required("GITHUB_REPO")),
            token: self.get("GITHUB_TOKEN").unwrap_or_default(),
        })
    }

    /// A comma-separated list, or `default`, trimmed and without empty entries.
    fn list(&self, key: &str, default: &str) -> Vec<String> {
        self.get(key)
            .unwrap_or_else(|| default.into())
            .split(',')
            .map(|entry| entry.trim().to_owned())
            .filter(|entry| !entry.is_empty())
            .collect()
    }

    /// The DEGIRO login whose variables start with `prefix`, or `None` without both a username and a password.
    fn login(&self, prefix: &str) -> Option<DegiroLogin> {
        let username = self.get(&format!("{prefix}_USERNAME"));
        let password = self.get(&format!("{prefix}_PASSWORD"));
        username.zip(password).map(|(name, secret)| DegiroLogin {
            password: secret,
            totp_secret: self.get(&format!("{prefix}_TOTP_SECRET")),
            username: name,
        })
    }

    /// A variable parsed as `T`, or `None` when unset or malformed.
    fn parsed<T>(&self, key: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.get(key)
            .as_deref()
            .and_then(|value| value.parse().ok())
    }

    /// The bucket from the variables `fly storage create` sets, and the query server's `INGEST_URL`.
    fn raw(&self, http: Client) -> Result<Raw> {
        let endpoint = tri!(
            tri!(self.required("AWS_ENDPOINT_URL_S3"))
                .parse()
                .context("AWS_ENDPOINT_URL_S3 is not a URL")
        );
        let region = self.get("AWS_REGION").unwrap_or_else(|| "auto".into());
        let bucket = tri!(
            Bucket::new(
                endpoint,
                UrlStyle::Path,
                tri!(self.required("BUCKET_NAME")),
                region
            )
            .context("bucket")
        );
        Ok(Raw {
            bucket,
            credentials: Credentials::new(
                tri!(self.required("AWS_ACCESS_KEY_ID")),
                tri!(self.required("AWS_SECRET_ACCESS_KEY")),
            ),
            http,
            ingest_url: self.get("INGEST_URL"),
        })
    }

    /// A variable that must be set.
    fn required(&self, key: &str) -> Result<String> {
        self.get(key).with_context(|| format!("{key} is not set"))
    }

    /// The fixer routine's trigger.
    fn routine(&self, http: Client) -> Routine {
        Routine {
            http,
            token: self.get("ROUTINE_TOKEN"),
            url: self.get("ROUTINE_FIRE_URL"),
        }
    }

    /// An origin, or `default`, without a trailing slash.
    fn url(&self, key: &str, default: &str) -> String {
        self.get(key).map_or_else(
            || default.into(),
            |url| url.trim_end_matches('/').to_owned(),
        )
    }
}

impl From<Vars> for Env {
    fn from(vars: Vars) -> Self {
        Self(vars.collect())
    }
}

/// Log a job's result and turn it into its report value.
fn outcome(name: &str, job: &str, result: Result<()>) -> Value {
    match result {
        Ok(()) => {
            info!("{name} {job}: ok");
            Value::String("ok".into())
        }
        Err(error) => {
            error!("{name} {job} failed: {error:#}");
            Value::String(format!("error: {error:#}"))
        }
    }
}
