//! Tests for the KNMI event catalogue source.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::Router;
use axum::extract::{Query, State};
use axum::routing::get;

use super::Earthquakes;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// The query of every request the fake FDSN service received.
type Asked = Arc<Mutex<Vec<HashMap<String, String>>>>;

/// The first run stores the whole catalogue from 1900, the second only the recent window.
#[tokio::test]
async fn stores_the_catalogue_once_then_recent_events() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let asked: Asked = Arc::default();
    let events =
        async |State(seen): State<Asked>, Query(query): Query<HashMap<String, String>>| -> String {
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(query);
            "#EventID|Time|Latitude\nknmi1|2026-10-18T09:35:23.3|53.187\n".to_owned()
        };
    let base = testing::serve(
        Router::new()
            .route("/fdsnws/event/1/query", get(events))
            .with_state(Arc::clone(&asked)),
    )
    .await;
    let app = testing::app_with(&github, |cfg| cfg.fdsn_url = base);
    tri!(Earthquakes.daily(&app).await);
    tri!(Earthquakes.daily(&app).await);
    let queries = asked.lock().unwrap_or_else(PoisonError::into_inner);
    ensure!(
        queries.len() == 2,
        "{} requests instead of 2",
        queries.len()
    );
    let first = tri!(queries.first().context("first request"));
    let second = tri!(queries.get(1).context("second request"));
    ensure!(
        first
            .get("starttime")
            .is_some_and(|start| start == "1900-01-01"),
        "first run from 1900"
    );
    ensure!(
        second
            .get("starttime")
            .is_some_and(|start| start != "1900-01-01"),
        "second run recent only"
    );
    ensure!(
        queries
            .iter()
            .all(|query| query.get("minlatitude").is_some_and(|lat| lat == "50.5")),
        "area filter sent"
    );
    drop(queries);
    let record = recorded.lock().unwrap_or_else(PoisonError::into_inner);
    ensure!(
        record.commit_messages == ["earthquakes: catalogue", "earthquakes: recent"],
        "one commit per run"
    );
    let first_path = tri!(
        record
            .trees
            .first()
            .and_then(|tree| tree.first())
            .context("first stored file")
    );
    ensure!(
        first_path.starts_with("raw/earthquakes/catalogue/")
            && Path::new(first_path)
                .extension()
                .is_some_and(|extension| extension == "txt"),
        "{first_path}"
    );
    drop(record);
    Ok(())
}
