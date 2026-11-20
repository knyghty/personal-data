//! Tests for `browser`.

use std::path::{Path, PathBuf};
use std::{env, fs, process};

use anyhow::{Context as _, Result, ensure};
use rusqlite::{Connection, params};
use serde_json::json;

use super::{Firefox, Orion, Safari};
use crate::apple::iso;
use crate::tri;

/// Create a places.sqlite as Firefox keeps it while running: WAL, exclusive lock held, so there is no `-shm`.
///
/// Visits are (url, microseconds since the Unix epoch, source).
///
/// # Errors
///
/// Fails when `SQLite` does.
pub(crate) fn places_db(path: &Path, visits: &[(&str, i64, i64)]) -> Result<Connection> {
    let conn = tri!(Connection::open(path));
    tri!(conn.execute_batch(
        "PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA journal_mode = WAL;
         CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url LONGVARCHAR, title LONGVARCHAR,
           rev_host LONGVARCHAR, visit_count INTEGER DEFAULT 0, hidden INTEGER DEFAULT 0 NOT NULL,
           typed INTEGER DEFAULT 0 NOT NULL, frecency INTEGER DEFAULT -1 NOT NULL, last_visit_date INTEGER,
           guid TEXT, foreign_count INTEGER DEFAULT 0 NOT NULL, url_hash INTEGER DEFAULT 0 NOT NULL);
         CREATE TABLE moz_historyvisits (id INTEGER PRIMARY KEY, from_visit INTEGER, place_id INTEGER,
           visit_date INTEGER, visit_type INTEGER, session INTEGER, source INTEGER DEFAULT 0 NOT NULL,
           triggeringPlaceId INTEGER);",
    ));
    for &(url, date, source) in visits {
        let _: usize = tri!(conn.execute(
            "INSERT INTO moz_places (url, title, rev_host, visit_count, last_visit_date)
             VALUES (?1, 'Page', 'gro.elpmaxe.', 1, ?2)",
            params![url, date],
        ));
        let _: usize = tri!(conn.execute(
            "INSERT INTO moz_historyvisits (from_visit, place_id, visit_date, visit_type, source)
             VALUES (0, last_insert_rowid(), ?1, 1, ?2)",
            params![date, source],
        ));
    }
    Ok(conn)
}

/// Read a locked profile's visits by id and skip a profile directory without a database.
#[test]
fn reads_firefox_profiles_while_locked() -> Result<()> {
    let dir = tri!(tmp("firefox"));
    let profile = dir.join("abc.dev-edition-default");
    tri!(fs::create_dir_all(&profile));
    tri!(fs::create_dir_all(dir.join("empty.default")));
    let _writer = tri!(places_db(
        &profile.join("places.sqlite"),
        &[
            ("https://example.org/?q=1", 1_791_000_000_123_456, 0),
            ("https://example.org/synced", 1_791_000_001_000_000, 1),
        ],
    ));
    ensure!(
        !profile.join("places.sqlite-shm").exists(),
        "an exclusive lock leaves no -shm"
    );
    let firefox = Firefox { profiles: &dir };
    let profiles = tri!(firefox.profiles());
    ensure!(
        profiles == ["abc.dev-edition-default"],
        "only the profile with a database"
    );
    let name = "abc.dev-edition-default";
    let rows = tri!(firefox.visits(name, 0, 10));
    ensure!(rows.len() == 2, "two visits");
    let first = tri!(rows.first().context("no first visit"));
    ensure!(first.url == "https://example.org/?q=1", "the URL is kept");
    ensure!(
        first.time == "2026-10-03T04:00:00.123Z",
        "microseconds convert to milliseconds"
    );
    let second = tri!(rows.get(1).context("no second visit"));
    ensure!(second.origin_or_source == Some(1), "source");
    ensure!(second.visit_type == Some(1), "visit_type");
    ensure!(
        tri!(firefox.visits(name, 1, 10)).len() == 1,
        "visits after id 1"
    );
    ensure!(
        tri!(firefox.visits(name, 0, 1)).len() == 1,
        "the limit caps"
    );
    ensure!(
        tri!(firefox.visits(name, 2, 10)).is_empty(),
        "nothing after the last id"
    );
    Ok(())
}

/// Read visits from a live WAL copy by id, including ones synced late and device columns.
#[test]
fn reads_safari_from_a_live_wal_copy_by_id() -> Result<()> {
    let dir = tri!(tmp("safari"));
    let db = dir.join("History.db");
    let url = "https://example.org/a?token=x#y";
    let writer = tri!(safari_db(
        &db,
        &[
            (url, 1000.0_f64, 0),
            ("https://example.org/b", 2000.0_f64, 0),
        ],
    ));
    ensure!(
        dir.join("History.db-wal").exists(),
        "the writer keeps a WAL"
    );
    let safari = Safari { db: &db };
    let rows = tri!(safari.visits(0, 10));
    ensure!(rows.len() == 2, "two visits");
    let first = tri!(rows.first().context("no first visit"));
    ensure!(first.url == url, "the full URL is kept");
    ensure!(first.time == iso(1000.0), "the time is converted");
    ensure!(first.origin_or_source == Some(0), "origin");
    ensure!(first.load_successful == Some(true), "load_successful");
    ensure!(first.device.is_none(), "no device columns");
    // A visit synced late carries an older time but a newer id, so it is still read.
    tri!(safari_visit(
        &writer,
        ("https://example.org/phone", 500.0_f64, 1)
    ));
    let later = tri!(safari.visits(2, 10));
    ensure!(later.len() == 1, "one visit after id 2");
    let synced = tri!(later.first().context("no synced visit"));
    ensure!(
        (synced.visit_id, synced.origin_or_source) == (3, Some(1)),
        "the synced visit"
    );
    ensure!(tri!(safari.visits(0, 1)).len() == 1, "the limit caps");
    ensure!(
        tri!(safari.visits(3, 10)).is_empty(),
        "nothing after the last id"
    );
    // Columns about devices are sent when the schema has them.
    tri!(writer.execute_batch("ALTER TABLE history_visits ADD COLUMN device_id TEXT;"));
    let _: usize = tri!(writer.execute(
        "UPDATE history_visits SET device_id = 'IPHONE' WHERE id = 3",
        [],
    ));
    let with_device = tri!(safari.visits(2, 10));
    ensure!(
        with_device.first().and_then(|visit| visit.device.as_ref())
            == Some(&json!({"device_id": "IPHONE"})),
        "device columns"
    );
    ensure!(
        tri!(
            Safari {
                db: &dir.join("missing.db")
            }
            .visits(0, 10)
        )
        .is_empty(),
        "a missing database reads as empty"
    );
    Ok(())
}

/// Read Orion visits by id with the time as RFC 3339 UTC and no referrer for 0.
#[test]
fn reads_orion_by_id() -> Result<()> {
    let dir = tri!(tmp("orion"));
    let db = dir.join("history");
    let conn = tri!(Connection::open(&db));
    tri!(conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE history_items (ID INTEGER PRIMARY KEY AUTOINCREMENT, URL TEXT, TITLE TEXT, HOST TEXT,
           LAST_VISIT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, VISIT_COUNT INTEGER, TYPED_COUNT INTEGER);
         CREATE TABLE visits (ID INTEGER PRIMARY KEY AUTOINCREMENT, HISTORY_ITEM_ID INTEGER,
           VISIT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, REFERRING_VISIT_ID INTEGER, TRANSITION_TYPE TEXT);
         INSERT INTO history_items (URL, TITLE) VALUES ('https://example.org/a?x=1', 'A');
         INSERT INTO visits (HISTORY_ITEM_ID, VISIT_TIME, REFERRING_VISIT_ID, TRANSITION_TYPE)
           VALUES (1, '2026-11-05 19:09:31', 0, 'typed'), (1, '2026-11-05 19:09:44', 1, 'link');",
    ));
    let orion = Orion { db: &db };
    let rows = tri!(orion.visits(0, 10));
    ensure!(rows.len() == 2, "two visits");
    let first = tri!(rows.first().context("no first visit"));
    ensure!(
        first.time == "2026-11-05T19:09:31.000Z",
        "the time is RFC 3339 UTC"
    );
    ensure!(first.from_visit.is_none(), "referrer 0 is none");
    ensure!(first.transition.as_deref() == Some("typed"), "transition");
    ensure!(first.title.as_deref() == Some("A"), "title");
    let second = tri!(rows.get(1).context("no second visit"));
    ensure!(second.from_visit == Some(1), "referrer");
    ensure!(
        tri!(orion.visits(1, 10)).len() == 1,
        "the cursor skips sent visits"
    );
    ensure!(
        tri!(
            Orion {
                db: &dir.join("missing")
            }
            .visits(0, 10)
        )
        .is_empty(),
        "a missing database reads as empty"
    );
    Ok(())
}

/// Create a Safari History.db in WAL mode with Safari's tables, and visits of (url, seconds since 2001, origin).
///
/// # Errors
///
/// Fails when `SQLite` does.
pub(crate) fn safari_db(path: &Path, visits: &[(&str, f64, i64)]) -> Result<Connection> {
    let conn = tri!(Connection::open(path));
    tri!(conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE history_items (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL UNIQUE,
           domain_expansion TEXT NULL, visit_count INTEGER NOT NULL);
         CREATE TABLE history_visits (id INTEGER PRIMARY KEY AUTOINCREMENT,
           history_item INTEGER NOT NULL REFERENCES history_items(id) ON DELETE CASCADE,
           visit_time REAL NOT NULL, title TEXT NULL, load_successful BOOLEAN NOT NULL DEFAULT 1,
           http_non_get BOOLEAN NOT NULL DEFAULT 0, synthesized BOOLEAN NOT NULL DEFAULT 0,
           redirect_source INTEGER NULL UNIQUE REFERENCES history_visits(id) ON DELETE CASCADE,
           redirect_destination INTEGER NULL UNIQUE REFERENCES history_visits(id) ON DELETE CASCADE,
           origin INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0,
           attributes INTEGER NOT NULL DEFAULT 0, score INTEGER NOT NULL DEFAULT 0);",
    ));
    for visit in visits {
        tri!(safari_visit(&conn, *visit));
    }
    Ok(conn)
}

/// Insert one visit, adding its item when the URL is new.
///
/// # Errors
///
/// Fails when `SQLite` does.
fn safari_visit(conn: &Connection, (url, time, origin): (&str, f64, i64)) -> Result<()> {
    let _: usize = tri!(conn.execute(
        "INSERT INTO history_items (url, visit_count) VALUES (?1, 1)
         ON CONFLICT (url) DO UPDATE SET visit_count = visit_count + 1",
        [url],
    ));
    let _: usize = tri!(conn.execute(
        "INSERT INTO history_visits (history_item, visit_time, title, origin)
         SELECT id, ?2, 'Title', ?3 FROM history_items WHERE url = ?1",
        params![url, time, origin],
    ));
    Ok(())
}

/// A fresh empty directory for one test.
fn tmp(name: &str) -> Result<PathBuf> {
    let dir = env::temp_dir().join(format!(
        "personal-data-mac-browser-{name}-{}",
        process::id()
    ));
    if dir.exists() {
        tri!(fs::remove_dir_all(&dir));
    }
    tri!(fs::create_dir_all(&dir));
    Ok(dir)
}
