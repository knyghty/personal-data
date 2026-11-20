//! Tests for the Moneybird source against a fake API.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, State};
use axum::http::uri::PathAndQuery;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{NaiveDate, NaiveTime};
use serde_json::{Value, json};

use super::{Client, Moneybird, versions_key};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing::{self, Recorded};
use crate::tri;

/// The fake API's state.
#[derive(Default)]
struct Fake {
    /// Whether every synchronization list answers 429.
    rate_limit: bool,
    /// Requests seen, as `METHOD path?query`.
    requests: Vec<String>,
    /// `updated_at` of each time entry.
    time_entries: BTreeMap<&'static str, &'static str>,
    /// Version of each sales invoice; bumped between runs to simulate an edit.
    versions: BTreeMap<&'static str, i64>,
}

/// The fake API's state, shared with its handlers.
type Shared = Arc<Mutex<Fake>>;

/// Lock the fake API's state, poisoned or not.
fn state(shared: &Shared) -> MutexGuard<'_, Fake> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Lock the fake GitHub's record, poisoned or not.
fn recorded(record: &testing::Shared) -> MutexGuard<'_, Recorded> {
    record.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The message of every commit the fake GitHub received.
fn commit_messages(record: &testing::Shared) -> Vec<String> {
    recorded(record).commit_messages.clone()
}

/// Every blob the fake GitHub received, parsed.
fn blobs(record: &testing::Shared) -> Result<Vec<Value>> {
    let texts = recorded(record).blobs.clone();
    let parsed: Result<Vec<Value>, serde_json::Error> = texts
        .iter()
        .map(|text| serde_json::from_str(text))
        .collect();
    parsed.context("stored blobs")
}

/// Record a request and check its bearer token.
fn record(shared: &Shared, headers: &HeaderMap, method: &str, uri: &Uri) {
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer mb-token"),
        "bearer token"
    );
    let target = uri
        .path_and_query()
        .map_or_else(|| uri.path(), PathAndQuery::as_str);
    state(shared).requests.push(format!("{method} {target}"));
}

/// The synchronization list of a resource: id and version of every item.
async fn sync_list(
    State(shared): State<Shared>,
    headers: HeaderMap,
    uri: Uri,
    Path((admin, resource)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    record(&shared, &headers, "GET", &uri);
    assert_eq!(admin, "123", "administration");
    let fake = state(&shared);
    if fake.rate_limit {
        return (StatusCode::TOO_MANY_REQUESTS, "Retry later").into_response();
    }
    if resource == "contacts" {
        assert_eq!(
            query.get("include_archived").map(String::as_str),
            Some("true"),
            "archived contacts included"
        );
        return Json(json!([{"id": "c1", "version": 5_u8}])).into_response();
    }
    let filter = query.get("filter").map_or("", String::as_str);
    assert!(filter.starts_with("period:20220101.."), "{filter}");
    if resource != "sales_invoices" {
        return Json(json!([])).into_response();
    }
    let items: Vec<Value> = fake
        .versions
        .iter()
        .map(|(id, version)| json!({"id": id, "version": version}))
        .collect();
    drop(fake);
    Json(Value::Array(items)).into_response()
}

/// What Moneybird answers a read-only token for any POST, the batch fetch included.
async fn read_only(State(shared): State<Shared>, headers: HeaderMap, uri: Uri) -> Response {
    record(&shared, &headers, "POST", &uri);
    (
        StatusCode::FORBIDDEN,
        r#"{"error":"Read-only access: write operations are not allowed"}"#,
    )
        .into_response()
}

/// One item, with the version the fake holds for it.
async fn item(
    State(shared): State<Shared>,
    headers: HeaderMap,
    uri: Uri,
    Path((_admin, resource, name)): Path<(String, String, String)>,
) -> Json<Value> {
    record(&shared, &headers, "GET", &uri);
    let id = name.trim_end_matches(".json");
    let version = state(&shared).versions.get(id).copied().unwrap_or(5);
    Json(json!({
        "id": id, "version": version, "resource": resource, "state": "open",
        "total_price_incl_tax": "121.0"
    }))
}

/// Serve the fake API with `fake` as its state, and an app pointed at it.
async fn setup(fake: Fake) -> (Arc<App>, testing::Shared, Shared) {
    let (github, commits) = testing::fake_github().await;
    let shared: Shared = Arc::new(Mutex::new(fake));
    let administrations =
        async |State(api): State<Shared>, headers: HeaderMap, uri: Uri| -> Json<Value> {
            record(&api, &headers, "GET", &uri);
            Json(json!([{"id": "123", "name": "Acme BV", "period_start_date": "2022-01-01"}]))
        };
    let time_entries = async |State(api): State<Shared>,
                              headers: HeaderMap,
                              uri: Uri,
                              Query(query): Query<HashMap<String, String>>|
           -> Json<Value> {
        record(&api, &headers, "GET", &uri);
        assert!(
            query.get("per_page").is_some_and(|size| size == "100")
                && query
                    .get("filter")
                    .is_some_and(|filter| filter.ends_with(",state:all")),
            "page size and state filter"
        );
        if query.get("page").map(String::as_str) != Some("1") {
            return Json(json!([]));
        }
        let entries: Vec<Value> = state(&api)
            .time_entries
            .iter()
            .map(|(id, at)| json!({"id": id, "updated_at": at, "description": "work"}))
            .collect();
        Json(Value::Array(entries))
    };
    let list = async |State(api): State<Shared>, headers: HeaderMap, uri: Uri| -> Json<Value> {
        record(&api, &headers, "GET", &uri);
        Json(json!([{"id": "x", "name": uri.path()}]))
    };
    let routes = Router::new()
        .route("/administrations.json", get(administrations))
        .route(
            "/{admin}/{resource}/synchronization.json",
            get(sync_list).post(read_only),
        )
        .route(
            "/{admin}/documents/{resource}/synchronization.json",
            get(sync_list).post(read_only),
        )
        .route("/{admin}/{resource}/{id}", get(item))
        .route("/{admin}/documents/{resource}/{id}", get(item))
        .route("/{admin}/time_entries.json", get(time_entries))
        .route("/{admin}/{resource}", get(list))
        .with_state(Arc::clone(&shared));
    let base = testing::serve(routes).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.moneybird_token = Some("mb-token".to_owned());
        cfg.moneybird_api_url = base;
    });
    (app, commits, shared)
}

/// New and edited items are fetched one by one and stored; unchanged ones are not asked again.
#[tokio::test]
async fn stores_new_and_changed_items_only() -> Result<()> {
    let fake = Fake {
        time_entries: [("t1", "2026-10-01T10:00:00Z")].into(),
        versions: [("inv1", 1), ("inv2", 1)].into(),
        ..Fake::default()
    };
    let (app, record, shared) = setup(fake).await;
    tri!(Moneybird.hourly(&app).await);
    let expected = [
        "moneybird: sales_invoice (2)",
        "moneybird: contact (1)",
        "moneybird: time_entry (1)",
    ];
    ensure!(
        commit_messages(&record) == expected,
        "one commit per resource"
    );
    let first_file = recorded(&record)
        .trees
        .first()
        .and_then(|tree| tree.first())
        .cloned();
    let first_path = tri!(first_file.context("first file"));
    ensure!(
        first_path.starts_with("raw/moneybird/sales_invoice/") && first_path.ends_with("-123.json"),
        "{first_path}"
    );
    let stored = tri!(blobs(&record));
    let first = tri!(stored.first().context("first blob"));
    ensure!(
        first.pointer("/request/ids").and_then(Value::as_u64) == Some(2),
        "two ids fetched"
    );
    ensure!(
        first.pointer("/response/0/id") == Some(&json!("inv1")),
        "first item"
    );
    ensure!(
        first.pointer("/response/0/total_price_incl_tax") == Some(&json!("121.0")),
        "item body stored"
    );
    let requests = state(&shared).requests.clone();
    ensure!(
        requests.iter().all(|request| request.starts_with("GET ")),
        "a read-only token allows no POST: {}",
        requests.join(" ")
    );
    ensure!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /123/sales_invoices/inv2.json")),
        "{}",
        requests.join(" ")
    );
    let key = versions_key("123", "sales_invoice");
    let versions: BTreeMap<String, Value> = tri!(app.store.get(&key).context("versions stored"));
    ensure!(
        versions.get("inv1").and_then(Value::as_u64) == Some(1),
        "inv1 version"
    );

    // Nothing changed: no commits, and only the synchronization lists are asked.
    state(&shared).requests.clear();
    tri!(Moneybird.hourly(&app).await);
    ensure!(commit_messages(&record).len() == 3, "no new commit");
    let unchanged = state(&shared).requests.clone();
    ensure!(
        unchanged.iter().all(|request| request.starts_with("GET ")),
        "{}",
        unchanged.join(" ")
    );

    // One invoice edited, one new, one deleted; one time entry edited.
    state(&shared).versions = [("inv2", 2), ("inv3", 1)].into();
    state(&shared).time_entries = [("t1", "2026-10-02T10:00:00Z")].into();
    tri!(Moneybird.hourly(&app).await);
    let later = ["moneybird: sales_invoice (2)", "moneybird: time_entry (1)"];
    ensure!(
        commit_messages(&record)
            .get(3..)
            .is_some_and(|rest| rest == later),
        "commits of the third run"
    );
    let all = tri!(blobs(&record));
    let last = tri!(all.last().context("last blob"));
    ensure!(
        last.pointer("/response/0/updated_at") == Some(&json!("2026-10-02T10:00:00Z")),
        "edited time entry stored"
    );
    let after: BTreeMap<String, Value> = tri!(app.store.get(&key).context("versions stored"));
    ensure!(after.len() == 2, "two invoices known");
    ensure!(!after.contains_key("inv1"), "deleted invoice forgotten");
    Ok(())
}

/// A 429 ends the run without storing versions; the next run fetches what was pending.
#[tokio::test]
async fn a_rate_limit_ends_the_run_and_keeps_the_state() -> Result<()> {
    let fake = Fake {
        rate_limit: true,
        versions: [("inv1", 1)].into(),
        ..Fake::default()
    };
    let (app, record, shared) = setup(fake).await;
    tri!(Moneybird.hourly(&app).await);
    ensure!(commit_messages(&record).is_empty(), "nothing committed");
    ensure!(
        app.store
            .get::<Value>(&versions_key("123", "sales_invoice"))
            .is_none(),
        "no versions stored"
    );
    state(&shared).rate_limit = false;
    tri!(Moneybird.hourly(&app).await);
    ensure!(
        commit_messages(&record).first().map(String::as_str)
            == Some("moneybird: sales_invoice (1)"),
        "pending invoice stored"
    );
    Ok(())
}

/// The daily lists are stored on the first run and skipped while unchanged.
#[tokio::test]
async fn daily_lists_are_stored_when_changed() -> Result<()> {
    let (app, record, _shared) = setup(Fake::default()).await;
    tri!(Moneybird.daily(&app).await);
    tri!(Moneybird.daily(&app).await);
    // The second run changes nothing, and a commit without files is skipped.
    ensure!(
        commit_messages(&record) == ["moneybird: daily (7 files)"],
        "one commit"
    );
    let first_tree = recorded(&record).trees.first().cloned();
    let paths = tri!(first_tree.context("first tree"));
    ensure!(
        paths
            .first()
            .is_some_and(|path| path.starts_with("raw/moneybird/administration/")),
        "administrations first"
    );
    ensure!(
        paths
            .iter()
            .any(|path| path.contains("/project/") && path.ends_with("-123.json")),
        "projects stored per administration"
    );
    let stored = tri!(blobs(&record));
    let third = tri!(stored.get(2).context("third blob"));
    ensure!(
        third.pointer("/request/pages").and_then(Value::as_u64) == Some(1),
        "page count recorded"
    );
    Ok(())
}

/// The period starts at the first bookkeeping year, at most nine years back.
#[tokio::test]
async fn period_is_at_most_ten_years_and_from_the_first_year() -> Result<()> {
    let (github, _record) = testing::fake_github().await;
    let app = testing::app(&github);
    let today = tri!(NaiveDate::from_ymd_opt(2026, 11, 1).context("date"));
    let client = Client::new(&app, "mb-token", today.and_time(NaiveTime::MIN).and_utc());
    ensure!(
        client.period(&json!({"period_start_date": "2022-01-01"})) == "20220101..20261101",
        "from the first year"
    );
    ensure!(
        client.period(&json!({"period_start_date": "2010-01-01"})) == "20170101..20261101",
        "capped at nine years back"
    );
    ensure!(
        client.period(&json!({})) == "20170101..20261101",
        "no start date"
    );
    Ok(())
}
