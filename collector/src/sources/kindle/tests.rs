//! Tests for `kindle`.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use rand_core::OsRng;
use rsa::RsaPrivateKey;
use rsa::pkcs1::EncodeRsaPrivateKey as _;
use rsa::pkcs8::LineEnding;
use serde_json::{Map, Value, json};

use super::{AUTH, Kindle, Library, Source as _};
use crate::testing;
use crate::tri;

/// A library sync reply with one ebook and one personal document.
const LIBRARY: &str = "<response><add_update_list>\
    <meta_data><ASIN>B1</ASIN><title>A</title><cde_contenttype>EBOK</cde_contenttype></meta_data>\
    <meta_data><ASIN>P1</ASIN><title>Doc</title><cde_contenttype>PDOC</cde_contenttype></meta_data>\
    </add_update_list></response>";

/// Keep the `EBOK` items and skip the personal documents.
#[test]
fn lists_ebooks_only() -> Result<()> {
    ensure!(
        Library(LIBRARY.to_owned()).ebooks() == ["B1"],
        "only the ebook"
    );
    Ok(())
}

/// Pull twice: each kind is committed once, and every record page is read on each run.
#[tokio::test]
async fn pulls_library_datasets_and_sidecars_once_per_change() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let asked: Arc<Mutex<Vec<String>>> = Arc::default();
    let log = Arc::clone(&asked);
    let base = testing::serve(
        Router::new()
            .route("/FionaTodoListProxy/syncMetaData", get(async || LIBRARY))
            .route(
                "/whispersync/v2/data/u/datasets",
                get(async || {
                    Json(json!({"datasets": [{"name": "WORDS", "namespace": "VocabBuilder", "identifier": "d1",
                        "links": {"records": "/whispersync/v2/data/u/datasets/d1/records"}}]}))
                }),
            )
            .route(
                "/whispersync/v2/data/u/datasets/d1/records",
                get(
                    async move |Query(params): Query<HashMap<String, String>>| {
                        let after = params.get("after").cloned().unwrap_or_default();
                        log.lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .push(format!("records after={after}"));
                        if params.contains_key("after") {
                            return Json(
                                json!({"records": {"k2": {"value": "2"}}, "links": {"next": null}}),
                            );
                        }
                        Json(
                            json!({"records": {"k1": {"value": "1"}}, "links": {"next": "/whispersync/v2/data/u/datasets/d1/records?after=1"}}),
                        )
                    },
                ),
            )
            .route(
                "/FionaCDEServiceEngine/sidecar",
                get(async |Query(params): Query<HashMap<String, String>>| {
                    Json(json!({"payload": {"key": params.get("key"), "records": []}}))
                }),
            ),
    )
    .await;
    let app = testing::app_with(&github, |cfg| {
        cfg.kindle_todo_url.clone_from(&base);
        cfg.kindle_cde_url.clone_from(&base);
        cfg.kindle_api_url = base;
    });
    let key = tri!(RsaPrivateKey::new(&mut OsRng, 2048));
    let pem = tri!(key.to_pkcs1_pem(LineEnding::LF));
    tri!(app.store.set(
        AUTH,
        &json!({"adp_token": "tok", "device_private_key": pem.as_str(), "user_id": "u"}),
    ));
    tri!(Kindle.daily(&app).await);
    tri!(Kindle.daily(&app).await);
    let record_pages = asked.lock().unwrap_or_else(PoisonError::into_inner).len();
    ensure!(
        record_pages == 4,
        "two pages of records per run, not {record_pages}"
    );
    let recorded_now = recorded.lock().unwrap_or_else(PoisonError::into_inner);
    let commit_messages = recorded_now.commit_messages.clone();
    let blobs = recorded_now.blobs.clone();
    drop(recorded_now);
    ensure!(
        commit_messages == ["kindle: daily (3 files)"],
        "unchanged data is not committed again"
    );
    let whispersync: Value = tri!(serde_json::from_str(tri!(
        blobs.get(1).context("no whispersync blob")
    )));
    ensure!(
        whispersync
            .pointer("/datasets/0/records")
            .and_then(Value::as_object)
            .map_or(0, Map::len)
            == 2,
        "both record pages merged"
    );
    let sidecars: Value = tri!(serde_json::from_str(tri!(
        blobs.get(2).context("no sidecars blob")
    )));
    ensure!(
        sidecars.pointer("/B1/key").and_then(Value::as_str) == Some("B1"),
        "the ebook's sidecar"
    );
    ensure!(sidecars.get("P1").is_none(), "no sidecar for the document");
    Ok(())
}
