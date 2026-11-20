//! Run the radar job against a fake WMS.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use chrono::{DateTime, Duration, DurationRound as _, SecondsFormat, Utc};
use serde_json::Value;

use super::{CURSOR, HORIZON, KnmiRadar, STEP};
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// The runs asked for, the one old step with no run (a gap), and the first unpublished step.
struct Fake {
    /// Reference time and time range of each request.
    asked: Vec<(String, String)>,
    /// The old step with no run.
    gap: DateTime<Utc>,
    /// The first step not published yet.
    published_until: DateTime<Utc>,
}

impl Fake {
    /// Rain for a published run, a `ServiceException` for a gap or an unpublished one.
    fn answer(&mut self, query: &HashMap<String, String>) -> Response {
        let (Some(reference), Some(span)) = (query.get("DIM_REFERENCE_TIME"), query.get("TIME"))
        else {
            return (StatusCode::BAD_REQUEST, "no reference time or time range").into_response();
        };
        let Ok(time) = reference.parse::<DateTime<Utc>>() else {
            return (StatusCode::BAD_REQUEST, "the reference time is not a time").into_response();
        };
        self.asked.push((reference.clone(), span.clone()));
        if time == self.gap || time >= self.published_until {
            let body = "<ServiceExceptionReport><ServiceException code=\"InvalidDimensionValue\">Store has no results.</ServiceException></ServiceExceptionReport>";
            return (StatusCode::NOT_FOUND, body).into_response();
        }
        format!(
            r#"[{{"name":"precipitationfc","data":{{"{reference}":{{"{reference}":"0.5"}}}}}}]"#
        )
        .into_response()
    }
}

/// The fake behind the WMS handler.
type Shared = Arc<Mutex<Fake>>;

/// Skip an old step with no run, and wait at the first unpublished step.
#[tokio::test]
async fn skips_old_gaps_and_waits_for_unpublished_steps() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let now = tri!(Utc::now().duration_trunc(STEP));
    let start = tri!(
        now.checked_sub_signed(Duration::hours(2))
            .context("two hours ago")
    );
    let unpublished = tri!(
        now.checked_sub_signed(Duration::minutes(10))
            .context("ten minutes ago")
    );
    let gap = tri!(
        start
            .checked_add_signed(STEP)
            .context("the step after the start")
    );
    let fake: Shared = Arc::new(Mutex::new(Fake {
        asked: Vec::new(),
        gap,
        published_until: unpublished,
    }));
    let wms = async |State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>| {
        shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .answer(&query)
    };
    let base = testing::serve(
        Router::new()
            .route("/wms/adaguc-server", get(wms))
            .with_state(Arc::clone(&fake)),
    )
    .await;
    let app = testing::app_with(&github, |cfg| {
        cfg.knmi_wms_key = Some("wms-key".to_owned());
        cfg.knmi_wms_url = base;
        cfg.home = Some((52.35_f64, 4.85_f64));
    });
    tri!(app.store.set(CURSOR, &start));
    tri!(KnmiRadar.hourly(&app).await);
    let minutes = unpublished.signed_duration_since(start).num_minutes();
    let steps = tri!(usize::try_from(tri!(
        minutes
            .checked_div(STEP.num_minutes())
            .context("five-minute steps")
    )));
    let asked = fake
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .asked
        .clone();
    ensure!(
        asked.len() == steps.saturating_add(1),
        "every step up to the unpublished one"
    );
    for (reference, span) in asked {
        let time: DateTime<Utc> = tri!(reference.parse());
        let end = tri!(time.checked_add_signed(HORIZON).context("the horizon"));
        ensure!(
            span == format!(
                "{reference}/{}",
                end.to_rfc3339_opts(SecondsFormat::Secs, true)
            ),
            "the run starts at its reference time and spans the whole forecast"
        );
    }
    ensure!(
        app.store.get::<DateTime<Utc>>(CURSOR) == Some(unpublished),
        "waits at the unpublished step"
    );
    let first_blob = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .blobs
        .first()
        .cloned();
    let blob = tri!(first_blob.context("a committed blob"));
    let replies: Vec<Value> = tri!(serde_json::from_str(&blob));
    ensure!(
        replies.len() == steps.saturating_sub(1),
        "the old gap is skipped"
    );
    ensure!(
        replies
            .first()
            .and_then(|reply| reply.pointer("/reply/0/name"))
            .and_then(Value::as_str)
            == Some("precipitationfc"),
        "the reply is stored verbatim"
    );
    Ok(())
}
