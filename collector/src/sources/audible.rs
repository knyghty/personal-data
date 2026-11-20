//! Pull the owner's Audible library, listening time and positions daily, through the API of the Audible apps.
//!
//! The API is unofficial; requests are signed with the key of a registered device, which does not expire until the
//! device is deregistered under Amazon's devices. The device's `adp_token` and `device_private_key`, from the auth file
//! of the `audible` Python library, live in the state under `audible/oauth` with the store's `domain` (de, co.uk, ...).
//! Each kind is committed only when changed.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{Duration, Months, Utc};
use reqwest::StatusCode;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey as _;
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding as _, Signer as _};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use url::Url;

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Directory name under `raw/`.
const SOURCE: &str = "audible";
/// Store key of the device registration.
const AUTH: &str = "audible/oauth";
/// Library items per page, the API's maximum.
const PAGE: usize = 1000;
/// Response groups asked for with each library item.
const LIBRARY_GROUPS: &str = "product_attrs,product_desc,product_extended_attrs,contributors,series,media,rating,\
                              listening_status,percent_complete,is_finished,is_returnable,origin_asin,pdf_url,categories";
/// Positions per request.
const ASINS_PER_REQUEST: usize = 50;

/// The Audible API of one store, called through a registered device.
struct Api<'app> {
    /// The app, for its HTTP client.
    app: &'app App,
    /// The API's base URL.
    base: String,
    /// The device that signs each request.
    device: Device,
}

impl Api<'_> {
    /// GET `path` with `params` and parse the JSON reply.
    async fn get(&self, path: &str, params: &[(&str, &str)]) -> Result<Value> {
        let url = tri!(Url::parse_with_params(
            &format!("{}{path}", self.base),
            params
        ));
        self.device.get_json(self.app, &url, "audible").await
    }

    /// Every library item, page by page.
    async fn library(&self) -> Result<Vec<Value>> {
        let mut items: Vec<Value> = Vec::new();
        for page in 1_u32.. {
            let reply = tri!(
                self.get(
                    "/1.0/library",
                    &[
                        ("num_results", &PAGE.to_string()),
                        ("page", &page.to_string()),
                        ("response_groups", LIBRARY_GROUPS),
                    ],
                )
                .await
            );
            let got = reply
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let last = got.len() < PAGE;
            items.extend(got);
            if last {
                break;
            }
        }
        Ok(items)
    }

    /// The last position heard of each of `asins`.
    async fn positions(&self, asins: &[&str]) -> Result<Vec<Value>> {
        let mut positions = Vec::new();
        for chunk in asins.chunks(ASINS_PER_REQUEST) {
            let reply = tri!(
                self.get(
                    "/1.0/annotations/lastpositions",
                    &[("asins", &chunk.join(","))],
                )
                .await
            );
            positions.extend(
                reply
                    .get("asin_last_position_heard_annots")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        Ok(positions)
    }

    /// Listening time per day for the last 30 days and per month for the last 12 months.
    async fn stats(&self) -> Result<Value> {
        let today = Utc::now().date_naive();
        let start_day = tri!(
            today
                .checked_sub_signed(Duration::days(29))
                .context("daily listening start")
        )
        .format("%Y-%m-%d")
        .to_string();
        let start_month = tri!(
            today
                .checked_sub_months(Months::new(11))
                .context("monthly listening start")
        )
        .format("%Y-%m")
        .to_string();
        self.get(
            "/1.0/stats/aggregates",
            &[
                ("daily_listening_interval_duration", "30"),
                ("daily_listening_interval_start_date", &start_day),
                ("monthly_listening_interval_duration", "12"),
                ("monthly_listening_interval_start_date", &start_month),
                ("store", "Audible"),
            ],
        )
        .await
    }
}

/// The Audible source.
pub(super) struct Audible;

#[async_trait]
impl Source for Audible {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(auth) = app.store.get::<Auth>(AUTH) else {
            return Ok(());
        };
        let api = Api {
            app,
            base: app
                .cfg
                .audible_api_url
                .clone()
                .unwrap_or_else(|| format!("https://api.audible.{}", auth.domain)),
            device: tri!(Device::new(&auth.adp_token, &auth.device_private_key)),
        };
        let items = tri!(api.library().await);
        let stats = tri!(api.stats().await);
        let asins: Vec<&str> = items
            .iter()
            .filter_map(|item| item.get("asin"))
            .filter_map(Value::as_str)
            .collect();
        let positions = tri!(api.positions(&asins).await);

        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for (kind, body) in [
            ("library", json!({"items": items})),
            ("stats", stats),
            (
                "positions",
                json!({"asin_last_position_heard_annots": positions}),
            ),
        ] {
            let text = body.to_string();
            let sha_key = format!("audible/sha/{kind}");
            if let Some(sha) = steam::changed(app, &sha_key, &text) {
                files.push(File {
                    path: storage::path(SOURCE, kind, now, None, "json"),
                    content: tri!(storage::envelope(
                        now,
                        json!({"domain": auth.domain}),
                        &text
                    )),
                });
                hashes.push((sha_key, sha));
            }
        }
        drop(tri!(
            app.save(&files, &format!("audible: daily ({} files)", files.len()))
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

/// The device registration in the store.
#[derive(Deserialize)]
struct Auth {
    /// The device's `adp_token`.
    adp_token: String,
    /// The device's private key, PKCS#1 PEM.
    device_private_key: String,
    /// The store's domain: de, co.uk, ...
    domain: String,
}

/// A registered Amazon device, which signs GET requests to Amazon's app APIs (Audible, Kindle).
pub(super) struct Device {
    /// The device's `adp_token`, sent and signed with every request.
    adp_token: String,
    /// The device's private key, ready to sign.
    signer: SigningKey<Sha256>,
}

impl Device {
    /// GET `url` signed, and return the body of a 2xx reply.
    pub(super) async fn get(&self, app: &App, url: &Url) -> Result<String> {
        tri!(self.get_found(app, url).await)
            .with_context(|| format!("GET {} 404 Not Found", url.path()))
    }

    /// Like `get`, but None for a 404.
    pub(super) async fn get_found(&self, app: &App, url: &Url) -> Result<Option<String>> {
        let path_and_query = url.query().map_or_else(
            || url.path().to_owned(),
            |query| format!("{}?{query}", url.path()),
        );
        let resp = tri!(
            app.http
                .get(url.clone())
                .header("x-adp-token", &self.adp_token)
                .header("x-adp-alg", "SHA256withRSA:1.0")
                .header("x-adp-signature", self.sign(&path_and_query))
                .send()
                .await
        );
        let status = resp.status();
        let text = tri!(resp.text().await);
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let snippet: String = text.chars().take(300).collect();
            bail!("GET {} {status}: {snippet}", url.path());
        }
        Ok(Some(text))
    }

    /// GET `url` signed and parse the JSON reply, with `what` as the error context.
    pub(super) async fn get_json(&self, app: &App, url: &Url, what: &'static str) -> Result<Value> {
        let text = tri!(self.get(app, url).await.context(what));
        Ok(tri!(serde_json::from_str(&text)))
    }

    /// Load the device's key from its PKCS#1 PEM.
    pub(super) fn new(adp_token: &str, device_private_key: &str) -> Result<Self> {
        let key = tri!(
            RsaPrivateKey::from_pkcs1_pem(device_private_key).context("decode device_private_key")
        );
        Ok(Self {
            adp_token: adp_token.to_owned(),
            signer: SigningKey::<Sha256>::new(key),
        })
    }

    /// The `x-adp-signature` header of a GET of `path_and_query` with an empty body.
    ///
    /// An RSA SHA-256 signature of method, path with query, time, body and token, one per line, then `:` and the time.
    /// The time copies Python's `isoformat()` with a `Z` appended, as the apps' library does.
    fn sign(&self, path_and_query: &str) -> String {
        let date = Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.6f+00:00Z")
            .to_string();
        let signed = format!("GET\n{path_and_query}\n{date}\n\n{}", self.adp_token);
        let signature = STANDARD.encode(self.signer.sign(signed.as_bytes()).to_bytes());
        format!("{signature}:{date}")
    }
}
