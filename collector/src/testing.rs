//! Test doubles: a fake GitHub API and an `App` wired to it.

use alloc::sync::Arc;
use core::net::SocketAddr;
use std::collections::HashMap;
use std::env::temp_dir;
use std::io::{self, Read as _};
use std::sync::{Mutex, MutexGuard, PoisonError};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use flate2::read::GzDecoder;
use rusty_s3::{Bucket, Credentials, UrlStyle};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::task::TaskTracker;
use tracing::error;
use uuid::Uuid;

use crate::app::{App, Config};
use crate::github::GitHub;
use crate::monitor::{Cron, Routine};
use crate::raw::Raw;
use crate::store::Store;
use crate::tri;

/// The admin secret of the test `App`.
pub(crate) const ADMIN: &str = "admin-secret";
/// The Apple Health secret of the test `App`.
pub(crate) const APPLE: &str = "apple-secret";
/// The Screen Time secret of the test `App`.
pub(crate) const SCREEN: &str = "screen-secret";
/// The network push secret of the test `App`.
pub(crate) const NETWORK: &str = "network-secret";

/// Everything the fake GitHub, bucket and query server received.
#[derive(Default)]
pub(crate) struct Recorded {
    /// Contents of each upload, gunzipped, in upload order.
    pub blobs: Vec<String>,
    /// Message of each ingest call, in call order.
    pub commit_messages: Vec<String>,
    /// Number of uploads still to fail with 504.
    pub fail_blobs: usize,
    /// Files the contents API serves, by repo-relative path.
    pub files: HashMap<String, String>,
    /// Payloads the fake routine endpoint received.
    pub fires: Vec<String>,
    /// Bucket key of each upload, in upload order.
    pub keys: Vec<String>,
    /// Paths named by each ingest call, in call order.
    pub trees: Vec<Vec<String>>,
}

/// The recorder, shared between the fake server and the test.
pub(crate) type Shared = Arc<Mutex<Recorded>>;

impl Recorded {
    /// Answer `GET contents/<path>` from `files`, base64 as GitHub sends it.
    fn contents(&self, path: &str) -> (StatusCode, Json<Value>) {
        self.files.get(path).map_or(
            (StatusCode::NOT_FOUND, Json(json!({"message": "Not Found"}))),
            |content| {
                let encoded = BASE64_STANDARD.encode(content);
                (
                    StatusCode::OK,
                    Json(json!({"content": encoded, "encoding": "base64"})),
                )
            },
        )
    }

    /// Answer `POST /ingest`: record the message and the paths.
    fn ingest(&mut self, body: &Value) {
        self.commit_messages.push(text(body, "/message"));
        let paths = body
            .get("paths")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|path| path.as_str().map(String::from))
            .collect();
        self.trees.push(paths);
    }

    /// Answer `PUT /bucket/<key>`: record the gunzipped body, or fail while `fail_blobs` lasts.
    fn put(&mut self, key: &str, body: &[u8]) -> StatusCode {
        if self.fail_blobs > 0 {
            self.fail_blobs = self.fail_blobs.saturating_sub(1);
            return StatusCode::GATEWAY_TIMEOUT;
        }
        let mut content = String::new();
        if GzDecoder::new(body).read_to_string(&mut content).is_err() {
            return StatusCode::BAD_REQUEST;
        }
        self.blobs.push(content);
        self.keys.push(key.to_owned());
        StatusCode::OK
    }
}

/// An `App` with the fake GitHub, a temporary state file, Sentry off, the fake routine and no sources configured.
pub(crate) fn app(github_api: &str) -> Arc<App> {
    app_with(github_api, |_| {})
}

/// Like `app`, with a hook to change the configuration first.
pub(crate) fn app_with(github_api: &str, tweak: impl FnOnce(&mut Config)) -> Arc<App> {
    let http = reqwest::Client::new();
    let dir = temp_dir().join(format!("collector-test-{}", Uuid::new_v4()));
    let mut cfg = Config {
        admin_secret: ADMIN.into(),
        albert_heijn_url: "http://localhost".into(),
        apple_health_secret: Some(APPLE.into()),
        audible_api_url: None,
        bunq_api_key: None,
        bunq_api_url: "http://localhost".into(),
        bunq_business_api_key: None,
        calendars: None,
        degiro: None,
        degiro_pension: None,
        degiro_url: "http://localhost".into(),
        fdsn_url: String::new(),
        github_activity_token: None,
        github_api: github_api.into(),
        github_user: None,
        gmail_api_url: "http://localhost".into(),
        gmail_full_senders: vec![],
        google_client_id: None,
        google_client_secret: None,
        google_portability_url: "http://localhost".into(),
        google_token_url: "http://localhost".into(),
        ha_backfill_days: 1,
        ha_location_entity: None,
        ha_notify: None,
        ha_stats_backfill_days: 1,
        ha_token: None,
        ha_url: None,
        home: None,
        kindle_api_url: "http://localhost".into(),
        kindle_cde_url: "http://localhost".into(),
        kindle_todo_url: "http://localhost".into(),
        knmi_edr_key: None,
        knmi_edr_url: "http://localhost".into(),
        knmi_url: String::new(),
        knmi_wms_key: None,
        knmi_wms_url: "http://localhost".into(),
        lastfm_api_key: None,
        lastfm_api_url: "http://localhost".into(),
        lastfm_user: None,
        luchtmeetnet_stations: vec![],
        luchtmeetnet_url: "http://localhost".into(),
        moneybird_api_url: "http://localhost".into(),
        moneybird_token: None,
        network_secret: Some(NETWORK.into()),
        offers_prijsprofeet: vec![],
        open_meteo_url: None,
        prijsprofeet_key: None,
        prijsprofeet_url: "http://localhost".into(),
        public_url: "http://localhost".into(),
        rws_url: String::new(),
        screen_time_secret: Some(SCREEN.into()),
        steam_api_key: None,
        steam_api_url: "http://localhost".into(),
        steam_id: None,
        twitch_api_url: "http://localhost".into(),
        twitch_client_id: None,
        twitch_client_secret: None,
        twitch_id_url: "http://localhost".into(),
        withings_backfill_from: None,
        withings_client_id: None,
        withings_client_secret: None,
        youtube_api_key: None,
        youtube_api_url: "http://localhost".into(),
        zwemwater_url: String::new(),
    };
    tweak(&mut cfg);
    let github = GitHub {
        api: github_api.to_owned(),
        branch: "main".into(),
        http: http.clone(),
        repo: "owner/repo".into(),
        token: "token".into(),
    };
    Arc::new(App {
        cfg,
        cron: Cron::default(),
        github,
        http: http.clone(),
        job_lock: AsyncMutex::new(()),
        raw: fake_raw(github_api, http.clone()),
        routine: Routine {
            http,
            token: Some("routine-token".into()),
            url: Some(format!("{github_api}/routines/trig_test/fire")),
        },
        store: Store::from(dir.join("state.json")),
        tasks: TaskTracker::new(),
    })
}

/// The fake bucket, path style under `/bucket`, and the query server's ingest route.
#[expect(
    clippy::single_call_fn,
    reason = "split out of `fake_github`, which is over `too_many_lines` otherwise"
)]
fn bucket_routes() -> Router<Shared> {
    Router::new()
        .route(
            "/bucket/{*key}",
            put(
                async |State(shared): State<Shared>, Path(key): Path<String>, body: Bytes| {
                    lock(&shared).put(&key, &body)
                },
            ),
        )
        .route(
            "/ingest",
            post(
                async |State(shared): State<Shared>, Json(body): Json<Value>| {
                    lock(&shared).ingest(&body);
                    StatusCode::ACCEPTED
                },
            ),
        )
}

/// The raw store on the fake server at `api`, which also serves the bucket and the ingest route.
#[expect(
    clippy::panic,
    reason = "a test setup that cannot build its fake bucket cannot run"
)]
#[expect(
    clippy::single_call_fn,
    reason = "split out of `app_with`, which is over `too_many_lines` otherwise"
)]
fn fake_raw(api: &str, http: reqwest::Client) -> Raw {
    let bucket = api
        .parse()
        .ok()
        .and_then(|endpoint| Bucket::new(endpoint, UrlStyle::Path, "bucket", "auto").ok());
    let Some(found) = bucket else {
        panic!("fake bucket at {api}");
    };
    Raw {
        bucket: found,
        credentials: Credentials::new("key", "secret"),
        http,
        ingest_url: Some(format!("{api}/ingest")),
    }
}

/// Start a fake GitHub API on a random port. Returns its base URL and the recorder.
pub(crate) async fn fake_github() -> (String, Shared) {
    let state: Shared = Arc::default();
    let router = Router::new()
        .route(
            "/repos/{owner}/{repo}/contents/{*path}",
            get(
                async |State(shared): State<Shared>,
                       Path((_, _, path)): Path<(String, String, String)>| {
                    lock(&shared).contents(&path)
                },
            ),
        )
        .route(
            "/routines/{id}/fire",
            post(
                async |State(shared): State<Shared>,
                       headers: HeaderMap,
                       Json(body): Json<Value>| {
                    if headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        != Some("Bearer routine-token")
                    {
                        return (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({"error": "bad token"})),
                        );
                    }
                    lock(&shared).fires.push(text(&body, "/text"));
                    (
                        StatusCode::OK,
                        Json(json!({
                            "type": "routine_fire",
                            "claude_code_session_url": "https://claude.ai/code/session_test",
                        })),
                    )
                },
            ),
        )
        .merge(bucket_routes())
        .with_state(Arc::clone(&state));
    let base = serve(router).await;
    (base, state)
}

/// Lock the recorder; a poisoned lock still holds what was recorded.
pub(crate) fn lock(shared: &Shared) -> MutexGuard<'_, Recorded> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Serve `router` on a random port and return its base URL.
pub(crate) async fn serve(router: Router) -> String {
    let bound: io::Result<(TcpListener, SocketAddr)> = async {
        let listener = tri!(TcpListener::bind("127.0.0.1:0").await);
        let addr = tri!(listener.local_addr());
        Ok((listener, addr))
    }
    .await;
    let (listener, addr) = match bound {
        Ok(pair) => pair,
        // Requests to this URL fail with the reason in their error.
        Err(error) => return format!("http://unbound/{error}"),
    };
    // Dropping the handle detaches the server task.
    drop(tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            error!("test server: {error}");
        }
    }));
    format!("http://{addr}")
}

/// The string at `pointer` in `body`, or empty.
fn text(body: &Value, pointer: &str) -> String {
    body.pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}
