//! Tests against the fake bucket and ingest route.

use anyhow::{Result, ensure};

use crate::storage::File;
use crate::testing;
use crate::tri;

/// A file to store.
fn file(path: &str, content: &str) -> File {
    File {
        content: content.into(),
        path: path.into(),
    }
}

/// Every file goes up gzipped under its path plus `.gz`, then one ingest call names them all.
#[tokio::test]
async fn uploads_files_and_tells_the_query_server() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    let app = testing::app(&api);
    let files = [file("raw/a/1.json", "{\"a\":1}"), file("raw/b/2.txt", "x")];
    let key = tri!(app.save(&files, "test: two files").await);
    ensure!(
        key.as_deref() == Some("raw/a/1.json.gz"),
        "the first file's key"
    );
    let seen = testing::lock(&recorded);
    ensure!(
        seen.keys == vec!["raw/a/1.json.gz", "raw/b/2.txt.gz"],
        "one key per file"
    );
    ensure!(
        seen.blobs == vec!["{\"a\":1}", "x"],
        "the contents, gunzipped"
    );
    ensure!(
        seen.trees == vec![vec!["raw/a/1.json", "raw/b/2.txt"]],
        "one ingest call with both paths"
    );
    ensure!(
        seen.commit_messages == vec!["test: two files"],
        "the ingest call carries the message"
    );
    drop(seen);
    Ok(())
}

/// An empty file list makes no requests and returns `None`.
#[tokio::test]
async fn nothing_to_store_is_none() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    let app = testing::app(&api);
    ensure!(
        tri!(app.save(&[], "empty").await).is_none(),
        "no key without files"
    );
    let seen = testing::lock(&recorded);
    ensure!(
        seen.keys.is_empty() && seen.commit_messages.is_empty(),
        "no upload and no ingest call"
    );
    drop(seen);
    Ok(())
}

/// A 5xx from the bucket is retried, and the file lands once.
#[tokio::test]
async fn retries_a_failed_upload() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    testing::lock(&recorded).fail_blobs = 2;
    let app = testing::app(&api);
    drop(tri!(app.save(&[file("raw/a.json", "a")], "m").await));
    ensure!(
        testing::lock(&recorded).keys == vec!["raw/a.json.gz"],
        "stored once after two failures"
    );
    Ok(())
}

/// Three 5xx answers in a row fail the save, and no ingest call goes out.
#[tokio::test]
async fn gives_up_after_three_failures() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    testing::lock(&recorded).fail_blobs = 3;
    let app = testing::app(&api);
    ensure!(
        app.save(&[file("raw/a.json", "a")], "m").await.is_err(),
        "the save fails"
    );
    ensure!(
        testing::lock(&recorded).commit_messages.is_empty(),
        "no ingest call for a failed upload"
    );
    Ok(())
}

/// A file added by hand goes up under its path and is named in an ingest call; paths outside raw/ are refused.
#[tokio::test]
async fn uploads_a_file_added_by_hand() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    let app = testing::app(&api);
    let key = tri!(app.raw.upload("raw/blood_test/report.json", b"{}").await);
    ensure!(key == "raw/blood_test/report.json.gz", "the key");
    for bad in ["raw/../state.json", "duckdb/tables.sql", "raw//a.json"] {
        ensure!(app.raw.upload(bad, b"x").await.is_err(), "{bad} is refused");
    }
    let seen = testing::lock(&recorded);
    ensure!(
        seen.keys == vec!["raw/blood_test/report.json.gz"],
        "one upload"
    );
    ensure!(
        seen.trees == vec![vec!["raw/blood_test/report.json"]],
        "one ingest call"
    );
    drop(seen);
    Ok(())
}
