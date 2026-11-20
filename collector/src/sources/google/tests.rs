//! Tests against a fake Google: token endpoint, Data Portability API and archive downloads.

use std::env;
use std::fs;
use std::io::Write as _;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::ensure;
use axum::extract::Path;
use axum::routing::post;
use axum::{Form, Json};
use flate2::Compression;
use flate2::write::DeflateEncoder;

use super::*;
use crate::testing;

/// What the fake Google saw: initiate bodies, and each job's state answer.
#[derive(Default)]
struct Fake {
    /// Where the fake serves downloads.
    base: String,
    /// Bodies of the initiate calls, in order.
    initiated: Vec<Value>,
    /// State every job reports.
    state: String,
    /// The subscriptions CSV in every snapshot archive.
    subscriptions: String,
}

/// The fake behind a lock.
type Shared = Arc<Mutex<Fake>>;

/// Lock a fake, poisoned or not.
fn lock<Inner>(shared: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An app wired to a fake GitHub and a fake Google, with the `brand` account approved.
async fn setup() -> Result<(Arc<App>, testing::Shared, Shared)> {
    let (github, recorded) = testing::fake_github().await;
    let fake: Shared = Arc::default();
    let initiate = async |State(shared): State<Shared>, Json(body): Json<Value>| -> Json<Value> {
        let mut seen = lock(&shared);
        seen.initiated.push(body);
        let number = seen.initiated.len();
        drop(seen);
        Json(json!({"archiveJobId": format!("job{number}")}))
    };
    let job_state = async |State(shared): State<Shared>, Path(id): Path<String>| -> Json<Value> {
        let seen = lock(&shared);
        let (state, base) = (seen.state.clone(), seen.base.clone());
        drop(seen);
        Json(json!({
            "state": state, "exportTime": "2026-10-17T10:00:00Z",
            "urls": [format!("{base}/download/{id}")],
        }))
    };
    let download = async |State(shared): State<Shared>, Path(id): Path<String>| -> Response {
        let subscriptions = lock(&shared).subscriptions.clone();
        // Each run starts activity, then snapshot: odd jobs are activity.
        let number: u32 = id.trim_start_matches("job").parse().unwrap_or(0);
        let files: &[(&str, &str)] = if number.rem_euclid(2) == 1 {
            &[
                (
                    "Portability/My Activity/YouTube/MyActivity.json",
                    "[{\"title\": \"Watched x\"}]",
                ),
                ("Portability/archive_browser.json", "{}"),
            ]
        } else {
            &[
                (
                    "Portability/YouTube and YouTube Music/subscriptions/subscriptions.csv",
                    subscriptions.as_str(),
                ),
                ("Portability/archive_browser.json", "{}"),
            ]
        };
        match zip(files) {
            Ok(bytes) => bytes.into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response(),
        }
    };
    let base = testing::serve(
        Router::new()
            .route("/token", post(token))
            .route("/v1/portabilityArchive:initiate", post(initiate))
            .route(
                "/v1/archiveJobs/{id}/portabilityArchiveState",
                get(job_state),
            )
            .route("/download/{id}", get(download))
            .with_state(Arc::clone(&fake)),
    )
    .await;
    lock(&fake).base = base.clone();
    lock(&fake).state = "IN_PROGRESS".to_owned();
    lock(&fake).subscriptions = "Channel Id\nUC1\n".to_owned();
    let app = testing::app_with(&github, |cfg| {
        cfg.google_client_id = Some("cid".to_owned());
        cfg.google_client_secret = Some("secret".to_owned());
        cfg.google_token_url = format!("{base}/token");
        cfg.google_portability_url = base;
    });
    let reply = token(Form(HashMap::from([
        ("client_id".to_owned(), "cid".to_owned()),
        ("grant_type".to_owned(), "authorization_code".to_owned()),
    ])))
    .await;
    let approval = AuthState {
        account: "brand".to_owned(),
        state: String::new(),
    };
    drop(tri!(approval.save_grant(&app, &reply.0)));
    Ok((app, recorded, fake))
}

/// The fake token endpoint: a code grants two resources, a refresh token mints an access token.
async fn token(Form(form): Form<HashMap<String, String>>) -> Json<Value> {
    assert_eq!(form.get("client_id").map(String::as_str), Some("cid"));
    if form.get("grant_type").map(String::as_str) == Some("authorization_code") {
        return Json(json!({
            "access_token": "at", "refresh_token": "rt", "refresh_token_expires_in": 604_800_u64,
            "scope": format!("{SCOPE_PREFIX}myactivity.youtube {SCOPE_PREFIX}youtube.subscriptions"),
        }));
    }
    assert_eq!(form.get("refresh_token").map(String::as_str), Some("rt"));
    Json(json!({"access_token": "at"}))
}

/// Build a zip archive with deflated entries and data descriptors, the way Google's look.
fn zip(files: &[(&str, &str)]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut directory = Vec::new();
    for &(name, content) in files {
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        tri!(encoder.write_all(content.as_bytes()));
        let packed = tri!(encoder.finish());
        let offset = tri!(u32::try_from(out.len()));
        let header = |signature: u32, in_directory: bool| -> Result<Vec<u8>> {
            let mut bytes = signature.to_le_bytes().to_vec();
            if in_directory {
                bytes.extend(20_u16.to_le_bytes());
            }
            bytes.extend(20_u16.to_le_bytes());
            bytes.extend(8_u16.to_le_bytes()); // data descriptor: sizes in the local header are 0
            bytes.extend(8_u16.to_le_bytes());
            bytes.extend([0_u8; 8]); // time, date, crc
            let packed_size = if in_directory {
                tri!(u32::try_from(packed.len()))
            } else {
                0_u32
            };
            bytes.extend(packed_size.to_le_bytes());
            let size = if in_directory {
                tri!(u32::try_from(content.len()))
            } else {
                0_u32
            };
            bytes.extend(size.to_le_bytes());
            bytes.extend(tri!(u16::try_from(name.len())).to_le_bytes());
            bytes.extend(0_u16.to_le_bytes());
            if in_directory {
                bytes.extend([0_u8; 10]); // comment length, disk, internal and external attributes
                bytes.extend(offset.to_le_bytes());
            }
            bytes.extend(name.as_bytes());
            Ok(bytes)
        };
        out.extend(tri!(header(0x0403_4b50, false)));
        out.extend(&packed);
        directory.extend(tri!(header(0x0201_4b50, true)));
    }
    let directory_offset = tri!(u32::try_from(out.len()));
    out.extend(&directory);
    out.extend(0x0605_4b50_u32.to_le_bytes());
    out.extend([0_u8; 4]);
    let count = tri!(u16::try_from(files.len()));
    out.extend(count.to_le_bytes());
    out.extend(count.to_le_bytes());
    out.extend(tri!(u32::try_from(directory.len())).to_le_bytes());
    out.extend(directory_offset.to_le_bytes());
    out.extend(0_u16.to_le_bytes());
    Ok(out)
}

/// Deflated entries with data descriptors unpack by name; other bytes are not an archive.
#[test]
fn unzips_deflated_entries() -> Result<()> {
    let archive = tri!(zip(&[("a/b.json", "[1, 2]"), ("c.csv", "x,y\n1,2\n")]));
    let files = tri!(Archive(&archive).unzip());
    ensure!(files.len() == 2, "two files");
    let first = tri!(files.first().context("first file"));
    ensure!(first.0 == "a/b.json" && first.1 == b"[1, 2]", "first file");
    let second = tri!(files.get(1).context("second file"));
    ensure!(second.1 == b"x,y\n1,2\n", "second file");
    ensure!(
        Archive(b"not a zip at all, just text").unzip().is_err(),
        "text is not a zip"
    );
    Ok(())
}

/// Check the reader on a real export: `GOOGLE_ZIP=<archive> cargo test real_export -- --ignored`.
#[test]
#[ignore = "needs GOOGLE_ZIP"]
fn unzips_a_real_export() -> Result<()> {
    let path = tri!(env::var("GOOGLE_ZIP"));
    let archive = tri!(fs::read(path));
    for (name, bytes) in tri!(Archive(&archive).unzip()) {
        info!("{name}\t{}", bytes.len());
    }
    Ok(())
}

/// Large arrays split into whole elements that concatenate back to the original.
#[test]
fn splits_large_arrays_into_whole_elements() -> Result<()> {
    ensure!(MAX_PART.split("[1, 2]").is_none(), "small files stay whole");
    let items: Vec<String> = (0_u32..20)
        .map(|number| format!("{{\"title\": \"Watched {number}\"}}"))
        .collect();
    let text = format!("[{}]", items.join(", "));
    ensure!(
        MAX_PART.split(&"x".repeat(100)).is_none(),
        "only JSON arrays are split"
    );
    let parts = tri!(MAX_PART.split(&text).context("a large array is split"));
    ensure!(
        parts.len() > 1 && parts.iter().all(|part| part.len() <= MAX_PART.0),
        "every part fits"
    );
    let mut back = Vec::new();
    for part in &parts {
        back.extend(tri!(serde_json::from_str::<Vec<Value>>(part)));
    }
    let original: Vec<Value> = tri!(serde_json::from_str(&text));
    ensure!(back == original, "the parts hold every element");
    Ok(())
}

/// Activity resources separate from snapshots, and scopes resolve known products only.
#[test]
fn splits_activity_from_snapshots() -> Result<()> {
    let grant = Grant {
        expires_at: None,
        refresh_token: String::new(),
        resources: [
            "myactivity.youtube",
            "youtube.subscriptions",
            "myactivity.maps",
        ]
        .map(String::from)
        .into(),
    };
    let [(_, activity), (_, snapshot)] = grant.split();
    ensure!(
        activity == ["myactivity.youtube", "myactivity.maps"],
        "activity resources"
    );
    ensure!(snapshot == ["youtube.subscriptions"], "snapshot resources");
    let scope = tri!(PRODUCTS.scopes(Some(&"youtube,search".to_owned())));
    ensure!(
        scope.ends_with("dataportability.myactivity.search"),
        "the search scope comes last"
    );
    ensure!(
        PRODUCTS.scopes(Some(&"gmail".to_owned())).is_err(),
        "unknown products fail"
    );
    Ok(())
}

/// Each day starts an activity and a snapshot export, polls them and commits the archives once.
#[tokio::test]
async fn exports_polls_and_commits_each_day() -> Result<()> {
    let (app, recorded, fake) = tri!(setup().await);
    tri!(Google.hourly(&app).await);
    let initiated = lock(&fake).initiated.clone();
    ensure!(
        initiated
            == [
                json!({"resources": ["myactivity.youtube"]}),
                json!({"resources": ["youtube.subscriptions"]})
            ],
        "one export per kind"
    );
    ensure!(lock(&recorded).trees.is_empty(), "nothing committed yet");

    // Still running: nothing new starts, nothing is committed.
    tri!(Google.hourly(&app).await);
    ensure!(
        lock(&fake).initiated.len() == 2,
        "no new exports while running"
    );

    lock(&fake).state = "COMPLETE".to_owned();
    tri!(Google.hourly(&app).await);
    let paths: Vec<String> = lock(&recorded).trees.iter().flatten().cloned().collect();
    ensure!(
        paths
            == [
                "raw/google/brand/2026/10/17/20261017T100000.000Z-my-activity-youtube-myactivity.json",
                "raw/google/brand/2026/10/17/20261017T100000.000Z-youtube-and-youtube-music-subscriptions-subscriptions.csv",
            ],
        "one file per export"
    );
    let first_blob = lock(&recorded).blobs.first().cloned();
    ensure!(
        first_blob.as_deref() == Some("[{\"title\": \"Watched x\"}]"),
        "the activity file is verbatim"
    );
    let jobs: Vec<Job> = tri!(app.store.get(&key("brand", "jobs")).context("jobs"));
    ensure!(jobs.is_empty(), "no jobs left");

    // A day later, activity starts from the last export, and an unchanged snapshot is not committed again.
    let yesterday = tri!(
        Utc::now()
            .checked_sub_signed(Duration::hours(25))
            .context("yesterday")
    );
    for kind in ["activity", "snapshot"] {
        tri!(
            app.store
                .set(&key("brand", &format!("started/{kind}")), &yesterday)
        );
    }
    tri!(Google.hourly(&app).await);
    let third = lock(&fake).initiated.get(2).cloned();
    ensure!(
        third
            == Some(
                json!({"resources": ["myactivity.youtube"], "startTime": "2026-10-17T09:00:00Z"})
            ),
        "activity resumes an hour before the last export"
    );
    tri!(Google.hourly(&app).await);
    let later = lock(&recorded);
    ensure!(
        later.trees.len() == 3,
        "one commit per export, none for the unchanged snapshot"
    );
    ensure!(
        later
            .trees
            .get(2)
            .and_then(|tree| tree.first())
            .is_some_and(|path| path.ends_with("-my-activity-youtube-myactivity.json")),
        "the third commit is the activity"
    );
    drop(later);
    Ok(())
}

/// A grant that ends within a day fails the run with a link to approve again, without the admin secret.
#[tokio::test]
async fn asks_to_approve_again_before_access_ends() -> Result<()> {
    let (app, _recorded, _fake) = tri!(setup().await);
    let mut grant: Grant = tri!(app.store.get(&key("brand", "oauth")).context("grant"));
    grant.expires_at = Utc::now().checked_add_signed(Duration::hours(3));
    tri!(app.store.set(&key("brand", "oauth"), &grant));
    let run = Google.hourly(&app).await;
    let Err(error) = run else {
        bail!("the run should fail");
    };
    let text = error.to_string();
    ensure!(
        text.contains("brand: access ends")
            && text.contains("/google/auth?account=brand&products=youtube&key="),
        "{text}"
    );
    ensure!(
        !text.contains(&app.cfg.admin_secret),
        "the secret stays out of the message"
    );
    Ok(())
}
