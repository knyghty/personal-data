//! Pull the owner's Kindle library, Whispersync data and per-book annotations daily, through the API of the Kindle apps.
//!
//! The API is unofficial, as for Audible; requests are signed with a registered "Kindle for iOS" device, which works
//! until it is removed under Amazon's devices. Its `adp_token`, `device_private_key` and Whispersync `user_id`
//! (`customer_info.user_id` of the registration) live in the state under `kindle/oauth`. Each kind is committed only
//! when changed:
//! - `library`: every book, from the library sync (XML, verbatim).
//! - `whispersync`: every dataset with all its records: read states, Vocabulary Builder words and look-ups, ...
//! - `sidecars`: per book, the reading positions, highlights, notes and bookmarks, as JSON.

#[cfg(test)]
mod tests;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::sync::Arc;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use axum::Router;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use url::Url;

use super::audible::Device;
use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Directory name under `raw/`.
const SOURCE: &str = "kindle";
/// Store key of the device registration.
const AUTH: &str = "kindle/oauth";
/// Books per library sync; the library is far smaller.
const LIBRARY_ITEMS: &str = "1000";
/// Record pages per Whispersync dataset; Amazon pages by 100, so this is far past any dataset.
const MAX_PAGES: usize = 1000;

/// The device registration in the store.
#[derive(Deserialize)]
struct Auth {
    /// The device's `adp_token`.
    adp_token: String,
    /// The device's private key, PKCS#1 PEM.
    device_private_key: String,
    /// The Whispersync user id.
    user_id: String,
}

impl Auth {
    /// Every Whispersync dataset with all its record pages merged.
    async fn datasets(&self, app: &App, device: &Device, api: &str) -> Result<Vec<Value>> {
        let datasets_url = tri!(Url::parse_with_params(
            &format!("{api}/whispersync/v2/data/{}/datasets", self.user_id),
            [("quiet", "true")],
        ));
        let listing = tri!(device.get_json(app, &datasets_url, "whispersync").await);
        let mut datasets = Vec::new();
        for dataset in listing
            .get("datasets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let mut records = Map::new();
            let mut next = dataset
                .pointer("/links/records")
                .and_then(Value::as_str)
                .map(String::from);
            let mut seen = BTreeSet::new();
            while let Some(link) =
                next.filter(|candidate| seen.insert(candidate.clone()) && seen.len() <= MAX_PAGES)
            {
                let url = tri!(Url::parse(&format!("{api}{link}")));
                let page = tri!(device.get_json(app, &url, "whispersync records").await);
                if let Some(found) = page.get("records").and_then(Value::as_object) {
                    records.extend(found.clone());
                }
                next = page
                    .pointer("/links/next")
                    .and_then(Value::as_str)
                    .map(String::from);
            }
            datasets.push(json!({
                "namespace": dataset.get("namespace"), "name": dataset.get("name"), "identifier": dataset.get("identifier"),
                "lastUpdatedTime": dataset.get("lastUpdatedTime"), "records": records,
            }));
        }
        Ok(datasets)
    }
}

/// The Kindle source.
pub(super) struct Kindle;

#[async_trait]
impl Source for Kindle {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(auth) = app.store.get::<Auth>(AUTH) else {
            return Ok(());
        };
        let device = tri!(Device::new(&auth.adp_token, &auth.device_private_key));
        let todo = app.cfg.kindle_todo_url.as_str();
        let api = app.cfg.kindle_api_url.as_str();

        let library_url = tri!(Url::parse_with_params(
            &format!("{todo}/FionaTodoListProxy/syncMetaData"),
            [("item_count", LIBRARY_ITEMS)],
        ));
        let library = Library(tri!(
            device
                .get(app, &library_url)
                .await
                .context("kindle library")
        ));

        let datasets = tri!(auth.datasets(app, &device, api).await);

        let mut sidecars = BTreeMap::new();
        for asin in library.ebooks() {
            let url = tri!(Url::parse_with_params(
                &format!("{}/FionaCDEServiceEngine/sidecar", app.cfg.kindle_cde_url),
                [("type", "EBOK"), ("key", asin.as_str())],
            ));
            // A book never opened on a device has no sidecar: Amazon answers 404.
            let Some(text) = tri!(
                device
                    .get_found(app, &url)
                    .await
                    .with_context(|| format!("kindle sidecar {asin}"))
            ) else {
                continue;
            };
            let mut reply: Value = tri!(serde_json::from_str(&text));
            let payload = reply
                .pointer_mut("/payload")
                .map_or(Value::Null, Value::take);
            drop(sidecars.insert(asin, payload));
        }

        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for (kind, body, ext) in [
            ("library", library.0, "xml"),
            (
                "whispersync",
                json!({"datasets": datasets}).to_string(),
                "json",
            ),
            ("sidecars", json!(sidecars).to_string(), "json"),
        ] {
            let sha_key = format!("kindle/sha/{kind}");
            if let Some(sha) = steam::changed(app, &sha_key, &body) {
                files.push(File {
                    path: storage::path(SOURCE, kind, now, None, ext),
                    content: body,
                });
                hashes.push((sha_key, sha));
            }
        }
        drop(tri!(
            app.save(&files, &format!("kindle: daily ({} files)", files.len()))
                .await
        ));
        for (key, sha) in hashes {
            tri!(app.store.set(&key, &sha));
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
        Router::new()
    }
}

/// A library sync reply, XML kept verbatim.
struct Library(String);

impl Library {
    /// The ASINs of the ebooks (`EBOK`) in the reply.
    fn ebooks(&self) -> Vec<String> {
        self.0
            .split("<meta_data>")
            .skip(1)
            .filter(|item| item.contains("<cde_contenttype>EBOK</cde_contenttype>"))
            .filter_map(|item| item.split("<ASIN>").nth(1))
            .filter_map(|rest| rest.split("</ASIN>").next())
            .map(ToOwned::to_owned)
            .collect()
    }
}
