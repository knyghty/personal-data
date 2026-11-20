//! Tests for `photos`.

use std::io::{self, Write as _};
use std::path::PathBuf;
use std::{env, fs, process};

use anyhow::{Context as _, Result, ensure};
use rusqlite::Connection;
use serde_json::{Value, json};

use super::{Photos, Sent};
use crate::tri;

/// Send labels once, resend them when they change, decode the index uuid and skip date categories.
#[test]
fn labels_resend_only_what_changed() -> Result<()> {
    let dir = tri!(tmp("labels"));
    tri!(fs::create_dir_all(dir.join("search")));
    let db = dir.join("Photos.sqlite");
    let uuid = "721BDE57-8671-4080-A39B-8BAE7DB178B7";
    let library = tri!(Connection::open(&db));
    tri!(library.execute_batch(&format!(
        "CREATE TABLE ZASSET (Z_PK INTEGER PRIMARY KEY, ZUUID TEXT);
         CREATE TABLE ZPERSON (Z_PK INTEGER PRIMARY KEY, ZPERSONUUID TEXT, ZFULLNAME TEXT, ZDISPLAYNAME TEXT);
         CREATE TABLE ZDETECTEDFACE (Z_PK INTEGER PRIMARY KEY, ZASSETFORFACE INTEGER, ZPERSONFORFACE INTEGER);
         INSERT INTO ZASSET VALUES (1, '{uuid}');
         INSERT INTO ZPERSON VALUES (1, 'P1', '', 'Sam');
         INSERT INTO ZDETECTEDFACE VALUES (1, 1, 1), (2, 1, NULL);"
    )));
    let psi = tri!(Connection::open(dir.join("search/psi.sqlite")));
    tri!(psi.execute_batch(
        "CREATE TABLE groups(category INT2, content_string TEXT);
         CREATE TABLE assets(uuid_0 INT, uuid_1 INT);
         CREATE TABLE ga(groupid INT, assetid INT);
         INSERT INTO assets VALUES (-9205232816531563662, -5226232214206047325);
         INSERT INTO groups VALUES (1500, 'Beach'), (1101, '2024'), (1203, 'Albert');
         INSERT INTO ga VALUES (1, 1), (2, 1), (3, 1);",
    ));

    let photos = Photos { db: &db };
    let (rows, sent) = tri!(photos.labels(&Sent::new(), 10));
    ensure!(rows.len() == 1, "one asset");
    let asset = tri!(rows.first().context("no asset"));
    ensure!(
        asset.get("uuid").and_then(Value::as_str) == Some(uuid),
        "the index uuid decodes to the asset's"
    );
    ensure!(
        asset.get("labels") == Some(&json!([[1203_i64, "Albert"], [1500_i64, "Beach"]])),
        "dates are skipped"
    );
    ensure!(
        asset.get("people") == Some(&json!([[null, null], ["P1", "Sam"]])),
        "a face without a person and a person without a full name"
    );
    ensure!(tri!(photos.labels(&sent, 10)).0.is_empty(), "unchanged");

    tri!(
        psi.execute_batch("INSERT INTO groups VALUES (1510, 'Dog'); INSERT INTO ga VALUES (4, 1);")
    );
    ensure!(tri!(photos.labels(&sent, 10)).0.len() == 1, "changed");
    Ok(())
}

/// Page assets by change and keep both of two assets that share a modification time across a page boundary.
#[test]
fn pages_through_assets_by_change() -> Result<()> {
    let dir = tri!(tmp("photos"));
    let db = dir.join("Photos.sqlite");
    let conn = tri!(Connection::open(&db));
    tri!(conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE ZASSET (Z_PK INTEGER PRIMARY KEY, ZUUID TEXT, ZDATECREATED REAL, ZADDEDDATE REAL,
                              ZMODIFICATIONDATE REAL, ZLATITUDE REAL, ZLONGITUDE REAL, ZKIND INTEGER);
         CREATE TABLE ZADDITIONALASSETATTRIBUTES (Z_PK INTEGER PRIMARY KEY, ZASSET INTEGER, ZTIMEZONENAME TEXT);
         CREATE TABLE ZEXTENDEDATTRIBUTES (Z_PK INTEGER PRIMARY KEY, ZASSET INTEGER, ZCAMERAMODEL TEXT);
         INSERT INTO ZASSET VALUES (1, 'B', 100, 100, 500, 52.37, 4.89, 0), (2, 'A', 101, 101, 500, NULL, NULL, 1),
                                   (3, 'C', 102, 102, 600, NULL, NULL, 0);
         INSERT INTO ZADDITIONALASSETATTRIBUTES VALUES (1, 1, 'Europe/Amsterdam');
         INSERT INTO ZEXTENDEDATTRIBUTES VALUES (1, 1, 'iPhone 15 Pro');",
    ));
    let photos = Photos { db: &db };
    // Two assets share a modification time; a page boundary between them loses neither.
    let (first, cursor) = tri!(photos.assets(&(0.0_f64, String::new()), 1));
    ensure!(
        first.first().and_then(|asset| asset.get("uuid")) == Some(&json!("A")),
        "the first page holds A"
    );
    let (rest, after) = tri!(photos.assets(&cursor, 10));
    ensure!(rest.len() == 2, "the rest");
    let second = tri!(rest.first().context("no asset"));
    ensure!(second.get("uuid") == Some(&json!("B")), "B follows A");
    ensure!(
        second.get("timezone") == Some(&json!("Europe/Amsterdam")),
        "additional attributes join"
    );
    ensure!(
        second.get("camera_model") == Some(&json!("iPhone 15 Pro")),
        "extended attributes join"
    );
    ensure!(second.get("lat") == Some(&json!(52.37_f64)), "lat");
    ensure!(
        second.get("lens").is_some_and(Value::is_null),
        "a column this macOS lacks is null"
    );
    ensure!(
        after == (600.0_f64, "C".to_owned()),
        "the cursor is the last asset"
    );
    ensure!(
        tri!(photos.assets(&after, 10)).0.is_empty(),
        "nothing after the cursor"
    );
    Ok(())
}

/// Check the query on a real library copy: `PHOTOS_DB=<Photos.sqlite> cargo test real_library -- --ignored`.
#[test]
#[ignore = "needs PHOTOS_DB, a copy of a real library"]
fn reads_a_real_library() -> Result<()> {
    let db = PathBuf::from(tri!(env::var("PHOTOS_DB")));
    let photos = Photos { db: &db };
    let (rows, cursor) = tri!(photos.assets(&(0.0_f64, String::new()), 100_000));
    let middle = rows
        .len()
        .checked_div(2)
        .and_then(|at| rows.get(at))
        .map_or_default(Value::to_string);
    let mut stdout = io::stdout();
    tri!(writeln!(
        stdout,
        "{} assets, cursor {} {}\n{middle}",
        rows.len(),
        cursor.0,
        cursor.1
    ));
    let (labelled, _) = tri!(photos.labels(&Sent::new(), 100_000));
    let middle_labelled = labelled
        .len()
        .checked_div(2)
        .and_then(|at| labelled.get(at))
        .map_or_default(Value::to_string);
    tri!(writeln!(
        stdout,
        "{} labelled, {} bytes\n{middle_labelled}",
        labelled.len(),
        tri!(serde_json::to_string(&labelled)).len(),
    ));
    Ok(())
}

/// A fresh empty directory for one test.
fn tmp(name: &str) -> Result<PathBuf> {
    let dir = env::temp_dir().join(format!("personal-data-mac-{name}-{}", process::id()));
    if dir.exists() {
        tri!(fs::remove_dir_all(&dir));
    }
    tri!(fs::create_dir_all(&dir));
    Ok(dir)
}
