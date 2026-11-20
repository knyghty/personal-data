//! Tests for `podcasts`.

use std::io::{self, Write as _};
use std::path::PathBuf;
use std::{env, fs, process};

use anyhow::{Context as _, Result, ensure};
use rusqlite::Connection;
use serde_json::{Value, json};

use super::{Podcasts, Sent};
use crate::tri;

/// Check the query on a real library copy: `PODCASTS_DB=<MTLibrary.sqlite> cargo test real_podcasts -- --ignored`.
#[test]
#[ignore = "needs PODCASTS_DB, a copy of a real library"]
fn reads_a_real_podcasts_library() -> Result<()> {
    let db = PathBuf::from(tri!(env::var("PODCASTS_DB")));
    let (rows, _) = tri!(Podcasts { db: &db }.episodes(&Sent::new(), 100_000));
    let middle = rows
        .len()
        .checked_div(2)
        .and_then(|at| rows.get(at))
        .map_or_default(Value::to_string);
    tri!(writeln!(
        io::stdout(),
        "{} episodes, {} bytes\n{middle}",
        rows.len(),
        tri!(serde_json::to_string(&rows)).len(),
    ));
    Ok(())
}

/// Send episodes with activity once, again when they change, and no more than the cap per push.
#[test]
fn sends_active_episodes_once_until_they_change() -> Result<()> {
    let dir = env::temp_dir().join(format!("personal-data-mac-podcasts-{}", process::id()));
    if dir.exists() {
        tri!(fs::remove_dir_all(&dir));
    }
    tri!(fs::create_dir_all(&dir));
    let db = dir.join("MTLibrary.sqlite");
    let conn = tri!(Connection::open(&db));
    tri!(conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE ZMTPODCAST (Z_PK INTEGER PRIMARY KEY, ZTITLE TEXT, ZAUTHOR TEXT, ZFEEDURL TEXT,
                                  ZSUBSCRIBED INTEGER);
         CREATE TABLE ZMTEPISODE (Z_PK INTEGER PRIMARY KEY, ZUUID TEXT, ZTITLE TEXT, ZPODCAST INTEGER,
                                  ZPUBDATE REAL, ZDURATION REAL, ZPLAYHEAD REAL, ZPLAYCOUNT INTEGER,
                                  ZPLAYSTATE INTEGER, ZLASTDATEPLAYED REAL, ZSAVED INTEGER);
         INSERT INTO ZMTPODCAST VALUES (1, 'The Intelligence', 'The Economist', 'https://x/rss', 1);
         INSERT INTO ZMTEPISODE VALUES (1, 'A', 'Played', 1, 700.0, 1200.0, 1200.0, 1, 2, 800.0, 0),
                                       (2, 'B', 'Never played', 1, 701.0, 900.0, 0.0, 0, 0, NULL, 0),
                                       (3, 'C', 'Saved', 1, 702.0, 900.0, 0.0, 0, 0, NULL, 1);",
    ));

    let podcasts = Podcasts { db: &db };
    let (rows, sent) = tri!(podcasts.episodes(&Sent::new(), 10));
    let uuids: Vec<Option<&str>> = rows
        .iter()
        .map(|row| row.get("uuid").and_then(Value::as_str))
        .collect();
    ensure!(
        uuids == [Some("A"), Some("C")],
        "an episode never touched is not sent"
    );
    let played = tri!(rows.first().context("no episode"));
    ensure!(
        played.get("podcast") == Some(&json!("The Intelligence")),
        "the podcast joins"
    );
    ensure!(
        played.get("play_count") == Some(&json!(1_u64)),
        "play_count"
    );
    ensure!(
        played.get("last_played") == Some(&json!(800.0_f64)),
        "last_played"
    );
    ensure!(
        played.get("bookmarked").is_some_and(Value::is_null),
        "a column this version lacks is null"
    );
    ensure!(tri!(podcasts.episodes(&sent, 10)).0.is_empty(), "unchanged");

    tri!(conn.execute_batch(
        "UPDATE ZMTEPISODE SET ZPLAYHEAD = 300.0, ZLASTDATEPLAYED = 900.0 WHERE ZUUID = 'C'",
    ));
    let (changed, _) = tri!(podcasts.episodes(&sent, 10));
    ensure!(changed.len() == 1, "one episode changed");
    ensure!(
        changed.first().and_then(|row| row.get("uuid")) == Some(&json!("C")),
        "the changed episode"
    );

    let (capped, partial) = tri!(podcasts.episodes(&Sent::new(), 1));
    ensure!(capped.len() == 1, "the cap");
    ensure!(
        partial.len() == 1,
        "the cap leaves the rest for the next push"
    );
    Ok(())
}
