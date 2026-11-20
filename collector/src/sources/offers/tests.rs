//! Run the daily pull and the Pi's push against fake `PrijsProfeet`, GitHub and Home Assistant servers.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::NaiveDate;
use serde_json::{Value, json};

use super::{Offers, SEEN_KEY, Source as _, WATCHLIST};
use crate::app::App;
use crate::testing::{self, NETWORK};
use crate::tri;

/// Three folderz cards as the Pi pushes them: a priced offer, a 1+1 without a price and a watched-but-excluded one.
const PUSH: &str = r#"[{"id": "folderz_69613305", "image": "https://img.example/lucovitaal.webp", "name": "Lucovitaal",
"original_price": 24.95, "price": 9.98, "quality": "none", "store": "kruidvat",
"url": "https://www.folderz.nl/winkels/kruidvat/aanbiedingen/lucovitaal-aanbieding-69613305/", "valid_from": null, "valid_until": "2099-11-08"},
{"id": "folderz_69613999", "image": null, "name": "Kruidvat Vitamine D3 & K2", "original_price": null, "price": null,
"quality": "good", "store": "kruidvat", "url": "https://www.folderz.nl/x", "valid_from": null, "valid_until": "2099-12-06"},
{"id": "folderz_69614000", "image": null, "name": "Kruidvat Vitamine D3 Druppels voor Kinderen", "original_price": 3.99, "price": 2.79,
"quality": "good", "store": "kruidvat", "url": "https://www.folderz.nl/y", "valid_from": null, "valid_until": "2099-11-08"}]"#;
/// The watch list the fake GitHub serves.
const WATCH: &str = r#"[{"label": "Dodoni yoghurt", "pattern": "dodoni", "stores": null, "till": "^DODONI"},
{"label": "Vitamine D", "pattern": "vitamine d3?\\b", "exclude": "kids|kind|druppels", "stores": ["kruidvat", "etos"], "till": null}]"#;

/// Notifications the fake Home Assistant received: service, authorization header and body.
type Told = Arc<Mutex<Vec<Value>>>;

/// The fakes and the app: the app's own routes, the fake GitHub's record and the notifications sent.
struct Fakes {
    /// The app, pulling `retailers` from the fake `PrijsProfeet`.
    app: Arc<App>,
    /// Base URL of the app's own routes, for the push.
    base: String,
    /// The fake GitHub's record.
    recorded: testing::Shared,
    /// Notifications the fake Home Assistant received.
    told: Told,
}

/// Fake `PrijsProfeet` and Home Assistant on one server, the fake GitHub with the watch list, and the app's routes.
async fn fakes(retailers: &[&str]) -> Fakes {
    let (github, recorded) = testing::fake_github().await;
    drop(
        testing::lock(&recorded)
            .files
            .insert(WATCHLIST.to_owned(), WATCH.to_owned()),
    );
    let told: Told = Arc::default();
    let router = Router::new()
        .route(
            "/products/promotional/all",
            get(
                async |Query(query): Query<HashMap<String, String>>,
                       headers: HeaderMap|
                       -> Response {
                    let retailer = query.get("retailer").cloned().unwrap_or_default();
                    if retailer == "limited" {
                        return (StatusCode::TOO_MANY_REQUESTS, "slow down").into_response();
                    }
                    if headers
                        .get("x-api-key")
                        .and_then(|value| value.to_str().ok())
                        != Some("k")
                    {
                        return (StatusCode::UNAUTHORIZED, "no key").into_response();
                    }
                    let products: Vec<Value> = if query.get("page").map(String::as_str) == Some("1")
                    {
                        (0_u32..100_u32).map(product).collect()
                    } else {
                        vec![product(100_u32)]
                    };
                    Json(json!({"total": 101_u32, "products": products})).into_response()
                },
            ),
        )
        .route(
            "/api/services/notify/{service}",
            post(
                async |State(sent): State<Told>,
                       Path(service): Path<String>,
                       headers: HeaderMap,
                       Json(body): Json<Value>| {
                    let auth = headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    sent.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(json!({"service": service, "auth": auth, "body": body}));
                    Json(json!([]))
                },
            ),
        )
        .with_state(Arc::clone(&told));
    let upstream = testing::serve(router).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.ha_notify = Some("mobile_app_test".into());
        cfg.ha_token = Some("ha-token".into());
        cfg.ha_url = Some(upstream.clone());
        cfg.offers_prijsprofeet = retailers
            .iter()
            .map(|retailer| (*retailer).to_owned())
            .collect();
        cfg.prijsprofeet_key = Some("k".into());
        cfg.prijsprofeet_url = upstream;
    });
    let base = testing::serve(Offers.router().with_state(Arc::clone(&app))).await;
    Fakes {
        app,
        base,
        recorded,
        told,
    }
}

/// The committed file whose content holds `needle`, parsed.
fn committed(recorded: &testing::Shared, needle: &str) -> Result<Value> {
    let blobs = testing::lock(recorded).blobs.clone();
    let blob = tri!(
        blobs
            .iter()
            .find(|blob| blob.contains(needle))
            .with_context(|| format!("no committed file holds {needle}"))
    );
    Ok(tri!(serde_json::from_str(blob)))
}

/// A `PrijsProfeet` product; number 0 is a watched one that starts next week.
fn product(number: u32) -> Value {
    let (name, from, until) = if number == 0 {
        (
            "Dodoni Griekse yoghurt 10%".to_owned(),
            "2099-11-09",
            "2099-11-15",
        )
    } else {
        (format!("AH Prei {number}"), "2026-11-02", "2099-11-08")
    };
    json!({
        "product_id": format!("ah_p{number}_2026-11-02"), "name": name, "retailer": "albert_heijn",
        "price": 0.39_f64, "original_price": 0.59_f64, "valid_from": from, "valid_until": until,
        "product_url": "https://www.ah.nl/producten/product/x", "image_url": "https://static.ah.nl/x.webp",
    })
}

/// POST `body` to the folderz push route for `store`, with `token` as bearer when given.
async fn push(
    base: &str,
    store: &str,
    token: Option<&str>,
    body: &str,
) -> Result<reqwest::Response> {
    let mut request = reqwest::Client::new()
        .post(format!("{base}/offers/folderz?store={store}"))
        .body(body.to_owned());
    if let Some(bearer) = token {
        request = request.bearer_auth(bearer);
    }
    Ok(tri!(request.send().await))
}

/// The notifications sent so far.
fn sent(told: &Told) -> Vec<Value> {
    told.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

/// File each pushed offer once, tell Home Assistant about the watched one, and refuse a bad push.
#[tokio::test]
async fn files_pushed_offers_once_and_tells_about_watched_ones() -> Result<()> {
    let run = fakes(&[]).await;
    ensure!(
        tri!(push(&run.base, "kruidvat", None, PUSH).await).status() == 401,
        "no token"
    );
    ensure!(
        tri!(push(&run.base, "Kruidvat!", Some(NETWORK), PUSH).await).status() == 400,
        "a store that is not a slug"
    );
    ensure!(
        tri!(push(&run.base, "kruidvat", Some(NETWORK), "{}").await).status() == 400,
        "a body that is not a list"
    );
    let response = tri!(push(&run.base, "kruidvat", Some(NETWORK), PUSH).await);
    ensure!(response.status() == 200, "accepted: {}", response.status());
    let reply: Value = tri!(response.json().await);
    ensure!(
        reply.pointer("/new") == Some(&json!(3_u32))
            && reply
                .pointer("/path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.starts_with("raw/offers/folderz/")
                    && path.ends_with("-kruidvat.json")),
        "the reply names the file and the count: {reply}"
    );
    let file = tri!(committed(&run.recorded, "folderz_69613305"));
    ensure!(
        file.pointer("/request/offers") == Some(&json!(3_u32))
            && file.pointer("/response/1/quality") == Some(&json!("good"))
            && file.pointer("/response/0/price") == Some(&json!(9.98_f64)),
        "the cards are filed as pushed: {file}"
    );
    let notifications = sent(&run.told);
    let message = notifications
        .first()
        .and_then(|sent| sent.pointer("/body/message"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    ensure!(
        notifications.len() == 1
            && notifications
                .first()
                .and_then(|sent| sent.pointer("/service"))
                == Some(&json!("mobile_app_test"))
            && notifications.first().and_then(|sent| sent.pointer("/auth"))
                == Some(&json!("Bearer ha-token"))
            && notifications
                .first()
                .and_then(|sent| sent.pointer("/body/title"))
                == Some(&json!("1 aanbieding van je lijst")),
        "one notification to the configured service: {notifications:?}"
    );
    ensure!(
        message == "Vitamine D: Kruidvat Vitamine D3 & K2 bij kruidvat t/m 6 Dec",
        "{message}"
    );

    let again: Value = tri!(
        tri!(push(&run.base, "kruidvat", Some(NETWORK), PUSH).await)
            .json()
            .await
    );
    ensure!(
        again.pointer("/new") == Some(&json!(0_u32))
            && again.pointer("/path") == Some(&Value::Null),
        "a second push files nothing: {again}"
    );
    ensure!(
        testing::lock(&run.recorded).commit_messages.len() == 1 && sent(&run.told).len() == 1,
        "and tells nothing new"
    );
    Ok(())
}

/// Page the retailer, file every product once, tell about the watched one, and keep going when a retailer fails.
#[tokio::test]
async fn pulls_each_retailer_once_and_keeps_what_succeeded() -> Result<()> {
    let run = fakes(&["limited", "albert_heijn"]).await;
    let error = tri!(
        Offers
            .daily(&run.app)
            .await
            .err()
            .context("a 429 was accepted")
    );
    let text = format!("{error:#}");
    ensure!(
        text.contains("prijsprofeet limited") && text.contains("429"),
        "{text}"
    );
    let paths = testing::lock(&run.recorded).trees.concat();
    ensure!(
        paths.len() == 1
            && paths
                .iter()
                .all(|path| path.starts_with("raw/offers/prijsprofeet/")
                    && path.ends_with("-albert_heijn.json")),
        "only the retailer that answered is filed: {paths:?}"
    );
    let ah = tri!(committed(&run.recorded, "ah_p100_2026-11-02"));
    ensure!(
        ah.pointer("/response")
            .and_then(Value::as_array)
            .map(Vec::len)
            == Some(101),
        "both pages' products are filed verbatim"
    );
    let seen: BTreeMap<String, NaiveDate> =
        tri!(run.app.store.get(SEEN_KEY).context("no seen set"));
    ensure!(
        seen.len() == 101,
        "its offers are remembered: {}",
        seen.len()
    );
    let message = sent(&run.told)
        .first()
        .and_then(|sent| sent.pointer("/body/message"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    ensure!(
        message
            == "Dodoni yoghurt: Dodoni Griekse yoghurt 10% \u{20ac}0.39 (was \u{20ac}0.59) bij albert heijn vanaf 9 Nov t/m 15 Nov",
        "{message}"
    );

    let fixed = fakes(&["albert_heijn"]).await;
    tri!(Offers.daily(&fixed.app).await);
    tri!(Offers.daily(&fixed.app).await);
    ensure!(
        testing::lock(&fixed.recorded).commit_messages.len() == 1 && sent(&fixed.told).len() == 1,
        "a second run files and tells nothing new"
    );
    Ok(())
}
