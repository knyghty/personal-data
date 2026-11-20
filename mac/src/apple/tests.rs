//! Tests for `apple`, and the knowledgeC, `sync.db` and `App.InFocus` fixtures the crate's tests build on.

use alloc::collections::{BTreeMap, BTreeSet};
use core::ops::Add as _;
use core::time::Duration;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use std::{env, process};

use anyhow::{Context as _, Result, bail, ensure};
use rusqlite::{Connection, params};
use serde_json::json;

use super::{APPLE_EPOCH, Biome, Focus, Knowledge, MAX_RAW_RECORDS, Stream, SyncDb, iso};
use crate::segb::tests::{Proto, proto, segb_v1, segb_v2};
use crate::tri;

/// Create a knowledgeC.db in WAL mode with the columns the query reads.
pub(crate) fn knowledge_db(
    path: &Path,
    rows: &[(&str, f64, f64, f64, Option<&str>)],
) -> Result<Connection> {
    let conn = tri!(Connection::open(path));
    tri!(conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE ZSOURCE (Z_PK INTEGER PRIMARY KEY, ZDEVICEID VARCHAR);
         CREATE TABLE ZOBJECT (Z_PK INTEGER PRIMARY KEY, ZSTREAMNAME VARCHAR, ZVALUESTRING VARCHAR,
           ZSTARTDATE TIMESTAMP, ZENDDATE TIMESTAMP, ZCREATIONDATE TIMESTAMP, ZSOURCE INTEGER);",
    ));
    for &(bundle, start, end, created, device) in rows {
        let _: usize = tri!(conn.execute("INSERT INTO ZSOURCE (ZDEVICEID) VALUES (?1)", [device]));
        let _: usize = tri!(conn.execute(
            "INSERT INTO ZOBJECT (ZSTREAMNAME, ZVALUESTRING, ZSTARTDATE, ZENDDATE, ZCREATIONDATE, ZSOURCE)
             VALUES ('/app/usage', ?1, ?2, ?3, ?4, last_insert_rowid())",
            params![bundle, start, end, created],
        ));
    }
    Ok(conn)
}

/// Create a Biome `sync.db` with the given `DevicePeer` rows: (identifier, name, platform code).
pub(crate) fn sync_db(path: &Path, peers: &[(&str, &str, i64)]) -> Result<()> {
    let conn = tri!(Connection::open(path));
    tri!(conn.execute_batch(
        "CREATE TABLE DevicePeer (device_identifier TEXT, name TEXT, platform INTEGER);",
    ));
    for &(id, name, platform) in peers {
        let _: usize = tri!(conn.execute(
            "INSERT INTO DevicePeer VALUES (?1, ?2, ?3)",
            params![id, name, platform],
        ));
    }
    Ok(())
}

/// A fresh, empty directory under the temp dir.
fn tmp(name: &str) -> Result<PathBuf> {
    let dir = env::temp_dir().join(format!("personal-data-mac-test-{name}-{}", process::id()));
    if dir.exists() {
        tri!(fs::remove_dir_all(&dir));
    }
    tri!(fs::create_dir_all(&dir));
    Ok(dir)
}

/// A written `App.InFocus` record: (state, time, data).
pub(crate) fn transition(gained: bool, time: f64, bundle: &str) -> (i32, f64, Vec<u8>) {
    (
        1,
        time,
        proto(&[
            Proto::Varint(3, u64::from(gained)),
            Proto::Double(4, time),
            Proto::Bytes(6, bundle.as_bytes()),
        ]),
    )
}

/// Send at most `MAX_RAW_RECORDS` records of a stream per push and the rest on the next.
#[test]
fn caps_raw_records_per_push_and_resumes() -> Result<()> {
    let biome = tri!(tmp("cap")).join("Biome");
    let name = "restricted/Notification.Usage";
    let dir = biome.join("streams").join(name);
    tri!(fs::create_dir_all(dir.join("local")));
    let record = proto(&[Proto::Varint(1, 1)]);
    let count = tri!(u32::try_from(MAX_RAW_RECORDS.saturating_add(2)).context("record count"));
    let records: Vec<(i32, f64, Vec<u8>)> = (0..count)
        .map(|number| (1_i32, 1_000.0_f64.add(f64::from(number)), record.clone()))
        .collect();
    tri!(fs::write(dir.join("local/1"), segb_v2(&records)));
    let stream = Stream {
        biome: &biome,
        name,
    };
    let mut cursors = BTreeMap::new();
    ensure!(
        tri!(stream.media(&mut cursors)).len() == MAX_RAW_RECORDS,
        "the first push is capped"
    );
    ensure!(
        tri!(stream.media(&mut cursors)).len() == 2,
        "the next push sends the rest"
    );
    ensure!(
        tri!(stream.media(&mut cursors)).is_empty(),
        "nothing is sent twice"
    );
    Ok(())
}

/// Format the Apple epoch and a fraction of a second, rounded to the millisecond.
#[test]
fn formats_apple_times() -> Result<()> {
    ensure!(iso(0.0) == "2001-01-01T00:00:00.000Z", "the Apple epoch");
    ensure!(
        iso(813_801_600.5) == "2026-10-16T00:00:00.500Z",
        "half a second"
    );
    ensure!(
        iso(813_801_600.062_5) == "2026-10-16T00:00:00.063Z",
        "rounded to the millisecond"
    );
    Ok(())
}

/// List the streams of both kinds and read a media stream's records as raw fields.
#[test]
fn lists_streams_and_reads_media_raw() -> Result<()> {
    let biome = tri!(tmp("media")).join("Biome");
    let now_playing = biome.join("streams/public/Media.NowPlaying");
    tri!(fs::create_dir_all(now_playing.join("local")));
    tri!(fs::create_dir_all(
        biome.join("streams/restricted/App.InFocus")
    ));
    let record = proto(&[Proto::Bytes(1, b"Song"), Proto::Varint(2, 5)]);
    tri!(fs::write(
        now_playing.join("local/1"),
        segb_v2(&[(1_i32, 500.0_f64, record)])
    ));
    let names = tri!(Biome { root: &biome }.streams());
    ensure!(
        names == ["public/Media.NowPlaying", "restricted/App.InFocus"],
        "both kinds, sorted"
    );
    let (Some(playing_name), Some(focus_name)) = (names.first(), names.get(1)) else {
        bail!("two streams, got {}", names.len());
    };
    let playing = Stream {
        biome: &biome,
        name: playing_name,
    };
    let in_focus = Stream {
        biome: &biome,
        name: focus_name,
    };
    ensure!(
        playing.is_media() && !in_focus.is_media(),
        "only the now-playing stream is media"
    );
    ensure!(
        playing.is_raw() && !in_focus.is_raw(),
        "only the now-playing stream is sent raw"
    );
    let mut cursors = BTreeMap::new();
    let rows = tri!(playing.media(&mut cursors));
    ensure!(rows.len() == 1, "one record");
    ensure!(
        rows.first().map(|row| &row.fields) == Some(&json!({"1": "Song", "2": 5_u64})),
        "the record's fields by number"
    );
    ensure!(
        cursors
            .get("public/Media.NowPlaying/local")
            .is_some_and(|at| at.total_cmp(&500.0_f64).is_eq()),
        "the cursor is the record time"
    );
    Ok(())
}

/// Read focus transitions of each device from v1 and v2 segments, skip junk, and send nothing twice.
#[test]
fn reads_focus_per_device_from_v1_and_v2_segments() -> Result<()> {
    let biome = tri!(tmp("biome"));
    let stream = biome.join("streams/restricted/App.InFocus");
    tri!(fs::create_dir_all(stream.join("local")));
    tri!(fs::create_dir_all(stream.join("remote/PHONE-UUID")));
    tri!(fs::write(
        stream.join("local/1"),
        segb_v1(&[transition(true, 100.0, "com.apple.Safari")]),
    ));
    tri!(fs::write(
        stream.join("remote/PHONE-UUID/1"),
        segb_v2(&[
            transition(true, 10_000.0, "com.burbn.instagram"),
            transition(false, 10_060.0, "com.burbn.instagram"),
        ]),
    ));
    tri!(fs::write(
        stream.join("remote/PHONE-UUID/junk"),
        b"not segb"
    ));
    let sync = biome.join("sync.db");
    tri!(sync_db(&sync, &[("PHONE-UUID", "Alex's iPhone", 2)]));
    let devices = tri!(
        SyncDb {
            db: &sync,
            host: "Mac",
        }
        .devices()
    );
    ensure!(devices.len() == 2, "this Mac and the phone");
    let mut cursors = BTreeMap::new();
    let in_focus = Stream {
        biome: &biome,
        name: "restricted/App.InFocus",
    };
    let rows = tri!(in_focus.focus(&devices, &mut cursors));
    ensure!(rows.len() == 3, "three transitions");
    let phone: Vec<&Focus> = rows
        .iter()
        .filter(|row| row.device_uuid == "PHONE-UUID")
        .collect();
    let (Some(first), Some(second)) = (phone.first(), phone.get(1)) else {
        bail!("two phone transitions, got {}", phone.len());
    };
    ensure!(
        first.device_name.as_deref() == Some("Alex's iPhone"),
        "the phone is named from sync.db"
    );
    ensure!(first.platform == "ios", "platform code 2 is iOS");
    ensure!(first.gained && !second.gained, "gained, then lost");
    ensure!(
        cursors
            .get("App.InFocus/PHONE-UUID")
            .is_some_and(|at| at.total_cmp(&10_060.0_f64).is_eq()),
        "the cursor is the newest transition"
    );
    // Far past the cursor, nothing is sent again.
    let _: Option<f64> = cursors.insert("App.InFocus/PHONE-UUID".to_owned(), 20_000.0_f64);
    let _: Option<f64> = cursors.insert("App.InFocus/local".to_owned(), 20_000.0_f64);
    ensure!(
        tri!(in_focus.focus(&devices, &mut cursors)).is_empty(),
        "nothing new"
    );
    Ok(())
}

/// Read rows that are still in the WAL of a live database, and only the overlap on the next run.
#[test]
fn reads_mac_usage_from_a_live_wal_copy() -> Result<()> {
    let dir = tri!(tmp("kc"));
    let db = dir.join("knowledgeC.db");
    // Keep the writer open so the rows stay in the WAL, as on a live Mac.
    let _writer = tri!(knowledge_db(
        &db,
        &[
            (
                "com.apple.Safari",
                1_000.0_f64,
                1_600.0_f64,
                1_601.0_f64,
                None
            ),
            (
                "com.apple.Safari",
                1_600.0_f64,
                1_600.0_f64,
                1_602.0_f64,
                None
            ),
            (
                "com.iphone.app",
                1_000.0_f64,
                1_200.0_f64,
                1_603.0_f64,
                Some("ABC"),
            ),
            (
                "com.apple.Terminal",
                7_000.0_f64,
                7_300.0_f64,
                7_301.0_f64,
                None
            ),
        ],
    ));
    ensure!(
        dir.join("knowledgeC.db-wal").exists(),
        "the rows are in the WAL"
    );
    let knowledge = Knowledge { db: &db };
    let first = tri!(knowledge.usage(0.0, &BTreeSet::new()));
    ensure!(
        first.cursor.total_cmp(&7_301.0_f64).is_eq(),
        "the cursor is the newest creation time"
    );
    let bundles: Vec<&str> = first.rows.iter().map(|row| row.bundle.as_str()).collect();
    ensure!(
        bundles == ["com.apple.Safari", "com.apple.Terminal"],
        "zero-length rows and other devices' rows are left out"
    );
    ensure!(
        first.rows.first().map(|row| row.start.as_str()) == Some(iso(1000.0).as_str()),
        "times are RFC 3339"
    );
    ensure!(
        first.seen.len() == 1,
        "only the Terminal row is within the overlap"
    );
    // The next run reads only the overlap, and skips what it sent.
    let overlap = tri!(knowledge.usage(7301.0, &BTreeSet::new()));
    ensure!(overlap.rows.len() == 1, "one row within the overlap");
    let again = tri!(knowledge.usage(7301.0, &first.seen));
    ensure!(again.rows.is_empty(), "the row was sent before");
    ensure!(again.seen == first.seen, "it stays in the seen set");
    let none = tri!(knowledge.usage(20_000.0, &BTreeSet::new()));
    ensure!(none.rows.is_empty(), "nothing after the cursor");
    ensure!(
        none.cursor.total_cmp(&20_000.0_f64).is_eq(),
        "the cursor stays put"
    );
    Ok(())
}

/// Read the newest segment file however old its modification time, because Biome appends in place.
#[test]
fn reads_the_live_segment_whatever_its_modification_time() -> Result<()> {
    let biome = tri!(tmp("live")).join("Biome");
    let name = "restricted/Media.NowPlaying";
    let dir = biome.join("streams").join(name);
    tri!(fs::create_dir_all(dir.join("local")));
    let record = proto(&[Proto::Varint(1, 1)]);
    // Created at 1,000 s and 50,000 s since 2001, in microseconds; both written to before the first push.
    tri!(fs::write(
        dir.join("local/1000000000"),
        segb_v2(&[(1_i32, 1_500.0_f64, record.clone())]),
    ));
    let live = dir.join("local/50000000000");
    tri!(fs::write(
        &live,
        segb_v2(&[(1_i32, 50_500.0_f64, record.clone())])
    ));
    let stream = Stream {
        biome: &biome,
        name,
    };
    let mut cursors = BTreeMap::new();
    ensure!(
        tri!(stream.media(&mut cursors)).len() == 2,
        "both records on the first push"
    );

    // Biome appends to the live file and leaves its modification time long before the cursor.
    let more = segb_v2(&[
        (1_i32, 50_500.0_f64, record.clone()),
        (1_i32, 90_000.0_f64, record),
    ]);
    tri!(fs::write(&live, more));
    let age = tri!(Duration::try_from_secs_f64(APPLE_EPOCH.add(1_000.0_f64)));
    let old = tri!(UNIX_EPOCH.checked_add(age).context("time out of range"));
    tri!(tri!(File::options().write(true).open(&live)).set_modified(old));
    let rows = tri!(stream.media(&mut cursors));
    ensure!(rows.len() == 1, "only the new record, from the live file");
    ensure!(
        cursors
            .get(&format!("{name}/local"))
            .is_some_and(|at| at.total_cmp(&90_000.0_f64).is_eq()),
        "the cursor moves to the new record"
    );
    Ok(())
}
