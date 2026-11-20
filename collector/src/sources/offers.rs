//! Store supermarket offers from the `PrijsProfeet` API and drugstore offers pushed by the Home Assistant Pi, and
//! tell Home Assistant when an item on the watch list goes on offer.
//!
//! The daily job pages the supermarkets. Folderz, where the drugstore offers come from, answers requests from the
//! server with a bot challenge, so `pi/push_offers.py` reads its list pages at home and POSTs the parsed cards to
//! `/offers/folderz`. Both paths store only the offers not seen before, one file per store, and remember their ids
//! until they expire. The watch list is `duckdb/watchlist.json` in the repo: patterns over offer names, each with
//! the stores it applies to. New offers that match it go to the `HA_NOTIFY` service as one notification.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use core::time::Duration;
use std::collections::HashMap;

use anyhow::{Context as _, Error, Result, bail};
use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::post;
use chrono::{DateTime, Days, NaiveDate, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::{info, warn};

use super::Source;
use super::home_assistant::Ha;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Days an expired offer's id stays in the seen set, so a store that lists it a little longer does not re-add it.
const GRACE_DAYS: u64 = 7;
/// Pages per retailer per run, a cap against a list that never ends.
const MAX_PAGES: u64 = 120;
/// Pause between `PrijsProfeet` requests with a key, which allows 150 a minute.
const PACE_KEYED: Duration = Duration::from_millis(if cfg!(test) { 0 } else { 500 });
/// Pause between `PrijsProfeet` requests without a key, which allows 30 a minute on list endpoints.
const PACE_PUBLIC: Duration = Duration::from_millis(if cfg!(test) { 0 } else { 2100 });
/// Offers per `PrijsProfeet` page, the API's maximum.
const PAGE_SIZE: u64 = 100;
/// Store key: offer ids already stored, each with the day its offer ends.
const SEEN_KEY: &str = "offers/seen";
/// Directory name under `raw/`.
const SOURCE: &str = "offers";
/// Days an offer without an end date counts as valid, for the seen set.
const UNKNOWN_VALIDITY_DAYS: u64 = 60;
/// Repo path of the watch list.
const WATCHLIST: &str = "duckdb/watchlist.json";

/// One offer, with the fields the alert and the folderz files use. The Pi pushes this shape.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Offer {
    /// The store's or the API's id, unique within the source, e.g. `ah_wi200513_2026-11-02` or `folderz_69613305`.
    pub id: String,
    /// The product image.
    pub image: Option<String>,
    /// The product name as the store prints it.
    pub name: String,
    /// The price before the offer, in EUR.
    pub original_price: Option<f64>,
    /// The offer price, in EUR. Missing when the card shows a mechanic such as 1+1 without a price.
    pub price: Option<f64>,
    /// Folderz's own rating of the offer: `excellent`, `good`, `average`, `poor` or `none`.
    pub quality: Option<String>,
    /// The store slug, e.g. `albert_heijn`, `kruidvat`.
    pub store: String,
    /// The offer's page.
    pub url: String,
    /// The first day the offer is valid; `PrijsProfeet` lists next week's offers a week early.
    pub valid_from: Option<NaiveDate>,
    /// The last day the offer is valid.
    pub valid_until: Option<NaiveDate>,
}

/// The offers source.
pub(super) struct Offers;

/// One run, a daily pull or a push: what it filed and what it knew before.
struct Pull<'app> {
    /// The app, for its HTTP client, configuration and store.
    app: &'app App,
    /// Offers first seen this run.
    new: Vec<Offer>,
    /// When the run started; names the files.
    now: DateTime<Utc>,
    /// Ids of stored offers with the day they stop being valid.
    seen: BTreeMap<String, NaiveDate>,
}

/// One watched item: a pattern over offer names and the stores it applies to.
struct Watch {
    /// Case-insensitive pattern that rules an offer out, for a `pattern` that is hard to narrow (kids' vitamins).
    exclude: Option<Regex>,
    /// Short name for the alert.
    label: String,
    /// Case-insensitive pattern over the offer name.
    pattern: Regex,
    /// Store slugs the pattern applies to; `None` is every store.
    stores: Option<Vec<String>>,
}

/// A watch list entry as written in `duckdb/watchlist.json`. `till`, the receipt pattern, is for the tables only.
#[derive(Deserialize)]
struct WatchEntry {
    /// Pattern that rules an offer out even when `pattern` matches; optional.
    #[serde(default)]
    exclude: Option<String>,
    /// Short name for the alert.
    label: String,
    /// Pattern over the offer name, matched without regard to case.
    pattern: String,
    /// Store slugs the pattern applies to; `null` is every store.
    stores: Option<Vec<String>>,
}

impl Offer {
    /// The alert line: label, name, price, old price, store, and the dates when it starts after `today` or ends.
    fn line(&self, label: &str, today: NaiveDate) -> String {
        let price = match (self.price, self.original_price) {
            (Some(price), Some(was)) => format!(" €{price:.2} (was €{was:.2})"),
            (Some(price), None) => format!(" €{price:.2}"),
            (None, _) => String::new(),
        };
        let from = self
            .valid_from
            .filter(|date| *date > today)
            .map_or_else(String::new, |date| {
                format!(" vanaf {}", date.format("%-d %b"))
            });
        let until = self.valid_until.map_or_else(String::new, |date| {
            format!(" t/m {}", date.format("%-d %b"))
        });
        format!(
            "{label}: {}{price} bij {}{from}{until}",
            self.name,
            self.store.replace('_', " ")
        )
    }
}

#[async_trait]
impl Source for Offers {
    async fn daily(&self, app: &App) -> Result<()> {
        if app.cfg.offers_prijsprofeet.is_empty() {
            info!("offers: no retailers configured, skipping");
            return Ok(());
        }
        let mut pull = Pull::from(app);
        let mut files = Vec::new();
        let mut errors = Vec::new();
        for retailer in &app.cfg.offers_prijsprofeet {
            match pull.prijsprofeet(retailer).await {
                Ok(file) => files.extend(file),
                Err(error) => errors.push(format!("prijsprofeet {retailer}: {error:#}")),
            }
        }
        let message = format!(
            "offers: daily ({} new, {} files)",
            pull.new.len(),
            files.len()
        );
        drop(tri!(pull.finish(&files, &message).await));
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(())
    }

    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        let ingest = async |State(app): State<Arc<App>>,
                            Query(query): Query<HashMap<String, String>>,
                            headers: HeaderMap,
                            body: Bytes|
               -> Response {
            let Some(secret) = app.cfg.network_secret.as_deref() else {
                return (StatusCode::SERVICE_UNAVAILABLE, "NETWORK_SECRET is not set")
                    .into_response();
            };
            if App::bearer(&headers) != Some(secret) {
                warn!(
                    bytes = body.len(),
                    "offers: rejected push, wrong or missing bearer token"
                );
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let store = query.get("store").map_or_default(String::as_str);
            let slug = !store.is_empty()
                && store.chars().all(|character| {
                    character.is_ascii_lowercase()
                        || character.is_ascii_digit()
                        || character == '-'
                        || character == '_'
                });
            if !slug {
                return (StatusCode::BAD_REQUEST, "store must be a slug").into_response();
            }
            let pushed: Vec<Offer> = match serde_json::from_slice(&body) {
                Ok(list) => list,
                Err(error) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("body is not a list of offers: {error}"),
                    )
                        .into_response();
                }
            };
            info!(
                bytes = body.len(),
                offers = pushed.len(),
                "offers: received {store} push"
            );
            match Pull::from(app.as_ref()).receive(store, pushed).await {
                Ok(reply) => Json(reply).into_response(),
                Err(error) => {
                    warn!("offers: {store} push failed: {error:#}");
                    (StatusCode::BAD_GATEWAY, format!("push failed: {error:#}")).into_response()
                }
            }
        };
        Router::new().route(
            "/offers/folderz",
            post(ingest).layer(DefaultBodyLimit::max(32 << 20)),
        )
    }
}

impl<'app> From<&'app App> for Pull<'app> {
    /// A run starting now, with the seen set less the ids that expired more than `GRACE_DAYS` ago.
    fn from(app: &'app App) -> Self {
        let now = Utc::now();
        let today = now.date_naive();
        let cutoff = today
            .checked_sub_days(Days::new(GRACE_DAYS))
            .unwrap_or(today);
        let mut seen: BTreeMap<String, NaiveDate> = app.store.get(SEEN_KEY).unwrap_or_default();
        seen.retain(|_, until| *until >= cutoff);
        Self {
            app,
            new: Vec::new(),
            now,
            seen,
        }
    }
}

impl Pull<'_> {
    /// Remember a store's new offers and return them as a file of `kind` named after the store.
    fn adopt(
        &mut self,
        kind: &str,
        store: &str,
        offers: Vec<Offer>,
        body: &str,
    ) -> Result<Option<File>> {
        if offers.is_empty() {
            info!("offers: {kind} {store}: nothing new");
            return Ok(None);
        }
        let today = self.now.date_naive();
        let fallback = today
            .checked_add_days(Days::new(UNKNOWN_VALIDITY_DAYS))
            .unwrap_or(today);
        info!("offers: {kind} {store}: {} new", offers.len());
        for offer in &offers {
            let _: Option<NaiveDate> = self
                .seen
                .insert(offer.id.clone(), offer.valid_until.unwrap_or(fallback));
        }
        let count = offers.len();
        self.new.extend(offers);
        Ok(Some(File {
            path: storage::path(SOURCE, kind, self.now, Some(store), "json"),
            content: tri!(storage::envelope(
                self.now,
                json!({"source": kind, "store": store, "offers": count}),
                body,
            )),
        }))
    }

    /// Send one notification for the new offers that match the watch list, when a notify service is configured.
    async fn alert(&self, watch: &[Watch]) -> Result<()> {
        let today = self.now.date_naive();
        let lines: Vec<String> = self
            .new
            .iter()
            .filter_map(|offer| {
                watch
                    .iter()
                    .find(|item| item.matches(offer))
                    .map(|item| offer.line(&item.label, today))
            })
            .collect();
        if lines.is_empty() {
            return Ok(());
        }
        let (Some(ha), Some(service)) = (Ha::new(self.app), self.app.cfg.ha_notify.as_deref())
        else {
            info!(
                "offers: {} watched offers, no HA_NOTIFY service to tell",
                lines.len()
            );
            return Ok(());
        };
        let title = if lines.len() == 1 {
            "1 aanbieding van je lijst".to_owned()
        } else {
            format!("{} aanbiedingen van je lijst", lines.len())
        };
        tri!(ha.notify(service, &title, &lines.join("\n")).await);
        info!("offers: told {service} about {} offers", lines.len());
        Ok(())
    }

    /// Commit `files`, save the seen set when something was filed, and send the alerts. Returns the commit sha.
    async fn finish(&self, files: &[File], message: &str) -> Result<Option<String>> {
        let sha = tri!(self.app.save(files, message).await);
        if !files.is_empty() {
            tri!(self.app.store.set(SEEN_KEY, &self.seen));
        }
        if !self.new.is_empty() {
            let watch = tri!(self.watch().await);
            tri!(self.alert(&watch).await);
        }
        Ok(sha)
    }

    /// GET `url` after a pause of `pace`, with the API key header when given. Returns the status and the body.
    async fn get(
        &self,
        url: &str,
        pace: Duration,
        key: Option<&str>,
    ) -> Result<(StatusCode, String)> {
        sleep(pace).await;
        let mut request = self.app.http.get(url);
        if let Some(value) = key {
            request = request.header("X-API-Key", value);
        }
        let resp = tri!(request.send().await.with_context(|| format!("GET {url}")));
        let status = resp.status();
        let text = tri!(resp.text().await);
        Ok((status, text))
    }

    /// Page a `PrijsProfeet` retailer's live promotions; file the unseen products verbatim, as a list.
    async fn prijsprofeet(&mut self, retailer: &str) -> Result<Option<File>> {
        let key = self.app.cfg.prijsprofeet_key.clone();
        let pause = if key.is_some() {
            PACE_KEYED
        } else {
            PACE_PUBLIC
        };
        let mut offers: Vec<Offer> = Vec::new();
        let mut products: Vec<Value> = Vec::new();
        let mut page = 1_u64;
        while page <= MAX_PAGES {
            let url = format!(
                "{}/products/promotional/all?retailer={retailer}&page={page}&page_size={PAGE_SIZE}",
                self.app.cfg.prijsprofeet_url
            );
            let (status, text) = tri!(self.get(&url, pause, key.as_deref()).await);
            if !status.is_success() {
                bail!(
                    "{url}: {status} {}",
                    text.chars().take(300).collect::<String>()
                );
            }
            let reply: Value = tri!(serde_json::from_str(&text).context("bad JSON"));
            let total = reply.get("total").and_then(Value::as_u64).unwrap_or(0);
            let listed = reply
                .get("products")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let count = u64::try_from(listed.len()).unwrap_or(u64::MAX);
            for product in listed {
                let Ok(offer) = Offer::try_from(&product) else {
                    continue;
                };
                if !self.seen.contains_key(&offer.id)
                    && !offers.iter().any(|got| got.id == offer.id)
                {
                    offers.push(offer);
                    products.push(product);
                }
            }
            if count < PAGE_SIZE || page.saturating_mul(PAGE_SIZE) >= total {
                break;
            }
            page = page.saturating_add(1);
        }
        let body = tri!(serde_json::to_string(&products));
        self.adopt("prijsprofeet", retailer, offers, &body)
    }

    /// File the pushed offers of `store` that were not seen, commit them and send the alerts. Returns the reply:
    /// the file's path, the commit sha and how many offers were new.
    async fn receive(&mut self, store: &str, pushed: Vec<Offer>) -> Result<Value> {
        let mut offers: Vec<Offer> = Vec::new();
        for mut offer in pushed {
            store.clone_into(&mut offer.store);
            if !self.seen.contains_key(&offer.id) && !offers.iter().any(|got| got.id == offer.id) {
                offers.push(offer);
            }
        }
        let body = tri!(serde_json::to_string(&offers));
        let count = offers.len();
        let files: Vec<File> = tri!(self.adopt("folderz", store, offers, &body))
            .into_iter()
            .collect();
        let message = format!("offers: folderz {store} ({count} new)");
        let sha = tri!(self.finish(&files, &message).await);
        Ok(json!({
            "path": files.first().map(|file| file.path.as_str()),
            "commit": sha,
            "new": count,
        }))
    }

    /// The watch list from the repo, patterns compiled.
    async fn watch(&self) -> Result<Vec<Watch>> {
        let text = tri!(self.app.github.file(WATCHLIST).await.context(WATCHLIST));
        let entries: Vec<WatchEntry> = tri!(serde_json::from_str(&text).context(WATCHLIST));
        entries.into_iter().map(Watch::try_from).collect()
    }
}

impl TryFrom<&Value> for Offer {
    type Error = Error;

    fn try_from(product: &Value) -> Result<Self> {
        let field = |key: &str| {
            product
                .get(key)
                .and_then(Value::as_str)
                .with_context(|| format!("product without {key}: {product}"))
        };
        Ok(Self {
            id: tri!(field("product_id")).to_owned(),
            image: field("image_url").ok().map(str::to_owned),
            name: tri!(field("name")).to_owned(),
            original_price: product.get("original_price").and_then(Value::as_f64),
            price: product.get("price").and_then(Value::as_f64),
            quality: None,
            store: tri!(field("retailer")).to_owned(),
            url: field("product_url").unwrap_or_default().to_owned(),
            valid_from: field("valid_from").ok().and_then(|date| date.parse().ok()),
            valid_until: field("valid_until").ok().and_then(|date| date.parse().ok()),
        })
    }
}

impl Watch {
    /// Whether `offer` is this item at one of its stores and not ruled out.
    fn matches(&self, offer: &Offer) -> bool {
        self.stores
            .as_ref()
            .is_none_or(|stores| stores.contains(&offer.store))
            && self.pattern.is_match(&offer.name)
            && self
                .exclude
                .as_ref()
                .is_none_or(|exclude| !exclude.is_match(&offer.name))
    }
}

impl TryFrom<WatchEntry> for Watch {
    type Error = Error;

    fn try_from(entry: WatchEntry) -> Result<Self> {
        let compile = |text: &str, what: &str| {
            Regex::new(&format!("(?i){text}"))
                .with_context(|| format!("watch {what} for {}", entry.label))
        };
        let pattern = tri!(compile(&entry.pattern, "pattern"));
        let exclude = tri!(
            entry
                .exclude
                .as_deref()
                .map(|text| compile(text, "exclude"))
                .transpose()
        );
        Ok(Self {
            exclude,
            label: entry.label,
            pattern,
            stores: entry.stores,
        })
    }
}
