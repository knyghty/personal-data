//! Pull the owner's DEGIRO portfolio, cash movements and transactions daily, through the web trader's API.
//!
//! Two accounts, each its own source: `degiro` and the pension account `degiro_pension`.
//!
//! DEGIRO has no official API. The calls follow the `degiro-connector` Python library (Chavithra/degiro-connector),
//! so they can break without notice. Each daily run logs in with the username, password and a one-time password from
//! the TOTP secret, reads the client's `intAccount`, and stores:
//! - `portfolio`: the trading update with the positions (`portfolio`), the account totals (`totalPortfolio`) and the
//!   money market funds (`cashFunds`), verbatim: one snapshot per day.
//! - `transaction`: the transaction history, and `account_overview`: the cash movements, per date window. The first
//!   run walks from 2010 in yearly windows; later runs fetch again from a week before the last run, since bookings
//!   land late. Empty windows are not stored. Files carry the window's start date as suffix.
//! - `product`: the description (name, ISIN, symbol, currency, type) of every product seen in those, once.
//!
//! The run logs out at the end. The password, the TOTP secret and the session id never reach the bucket, the logs or
//! Sentry: stored requests name the endpoint and window only, and errors drop the URL.

use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, Months, NaiveDate, Utc};
use hmac::{Hmac, Mac as _};
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, ORIGIN, REFERER, USER_AGENT};
use serde_json::{Value, json};
use sha1::Sha1;
use tracing::{info, warn};

use super::Source;
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

#[cfg(test)]
mod tests;

/// Where the first run starts; DEGIRO opened in 2008 and the owner's account is younger.
const HISTORY_START: NaiveDate = match NaiveDate::from_ymd_opt(2010, 1, 1) {
    Some(date) => date,
    // The date is valid, so this arm is never taken.
    None => NaiveDate::MIN,
};
/// Days fetched again each run, since cash movements are booked days after the fact.
const OVERLAP_DAYS: i64 = 7;
/// The pension account.
pub(super) const PENSION: Degiro = Degiro {
    name: "degiro_pension",
};
/// The personal account.
pub(super) const PERSONAL: Degiro = Degiro { name: "degiro" };
/// Product ids per `products/info` request.
const PRODUCTS_PER_REQUEST: usize = 50;
/// The reports fetched per date window.
const REPORTS: [Report; 2] = [
    Report {
        endpoint: "portfolio-reports/secure/v4/transactions",
        extra: &[("groupTransactionsByOrder", "false")],
        items: "/data",
        kind: "transaction",
        label: "degiro transactions",
    },
    Report {
        endpoint: "portfolio-reports/secure/v6/accountoverview",
        extra: &[],
        items: "/data/cashMovements",
        kind: "account_overview",
        label: "degiro account overview",
    },
];

/// A logged-in web trader session of one account.
struct Api<'app> {
    /// HTTP, the store and GitHub.
    app: &'app App,
    /// The web trader's base URL.
    base: &'app str,
    /// The client's account number, 0 until the client details are read.
    int_account: i64,
    /// When the run started; the files are stamped with it.
    now: DateTime<Utc>,
    /// The session id, a secret.
    session_id: String,
    /// The account.
    source: &'app Degiro,
}

impl Api<'_> {
    /// Consecutive windows of at most a year from `from` up to and including today.
    fn date_windows(&self, from: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
        let today = self.now.date_naive();
        let mut windows = Vec::new();
        let mut start = from.min(today);
        loop {
            let end = start
                .checked_add_months(Months::new(12))
                .and_then(|next| next.pred_opt())
                .unwrap_or(today)
                .min(today);
            windows.push((start, end));
            if end >= today {
                return windows;
            }
            start = end.succ_opt().unwrap_or(today);
        }
    }

    /// GET with the session as DEGIRO wants it, as query parameters. `what` names the call in errors.
    async fn get(&self, what: &str, url: &str, params: &[(&str, &str)]) -> Result<String> {
        let mut query = self.session_params();
        query.extend(params.iter().map(|&(name, value)| (name, value.to_owned())));
        let request = self
            .app
            .http
            .get(url)
            .headers(headers(self.base))
            .query(&query);
        send(what, request).await
    }

    /// End the session.
    async fn logout(&self) -> Result<()> {
        let request = self
            .app
            .http
            .put(format!(
                "{}/trading/secure/logout;jsessionid={}",
                self.base, self.session_id
            ))
            .headers(headers(self.base))
            .query(&self.session_params());
        send("trading/secure/logout", request).await.map(drop)
    }

    /// Today's trading update as a file, with the ids of the products held added to `product_ids`.
    async fn portfolio(&self, product_ids: &mut BTreeSet<String>) -> Result<File> {
        let url = format!(
            "{}/trading/secure/v5/update/{};jsessionid={}",
            self.base, self.int_account, self.session_id
        );
        let params = [
            ("portfolio", "0"),
            ("totalPortfolio", "0"),
            ("cashFunds", "0"),
        ];
        let update = tri!(self.get("trading/secure/v5/update", &url, &params).await);
        let parsed: Value = tri!(serde_json::from_str(&update).context("degiro update"));
        let rows = parsed.pointer("/portfolio/value").and_then(Value::as_array);
        product_ids.extend(
            rows.into_iter()
                .flatten()
                .filter_map(|row| row.get("id").and_then(Value::as_str))
                .filter(|id| id.bytes().all(|byte| byte.is_ascii_digit()))
                .map(str::to_owned),
        );
        let request = json!({"endpoint": "trading/secure/v5/update", "portfolio": 0_u8, "totalPortfolio": 0_u8, "cashFunds": 0_u8});
        self.stored("portfolio", None, request, &update)
    }

    /// The descriptions of the `unseen` products as files, `PRODUCTS_PER_REQUEST` per file.
    async fn products(&self, unseen: &[String]) -> Result<Vec<File>> {
        let mut files = Vec::new();
        for chunk in unseen.chunks(PRODUCTS_PER_REQUEST) {
            let lookup = self
                .app
                .http
                .post(format!(
                    "{}/product_search/secure/v5/products/info",
                    self.base
                ))
                .headers(headers(self.base))
                .query(&self.session_params())
                .json(&json!(chunk));
            let text = tri!(send("product_search/secure/v5/products/info", lookup).await);
            let request =
                json!({"endpoint": "product_search/secure/v5/products/info", "ids": chunk});
            let suffix = files.len().saturating_add(1).to_string();
            files.push(tri!(self.stored("product", Some(&suffix), request, &text)));
        }
        Ok(files)
    }

    /// Store today's portfolio, the new windows of transactions and cash movements, and unseen products.
    async fn pull(&self) -> Result<()> {
        let app = self.app;
        let name = self.source.name;
        let mut product_ids: BTreeSet<String> = BTreeSet::new();
        let mut files = vec![tri!(self.portfolio(&mut product_ids).await)];
        let mut counts = vec![("portfolio", 1_usize)];
        let from: NaiveDate = app
            .store
            .get(&self.source.history_key())
            .unwrap_or(HISTORY_START);
        let windows = self.date_windows(from);
        for spec in &REPORTS {
            let stored = tri!(self.report(spec, &windows, &mut product_ids).await);
            counts.push((spec.kind, stored.len()));
            files.extend(stored);
        }
        let seen: BTreeSet<String> = app
            .store
            .get(&self.source.products_key())
            .unwrap_or_default();
        let unseen: Vec<String> = product_ids.difference(&seen).cloned().collect();
        let described = tri!(self.products(&unseen).await);
        let products = described.len();
        counts.push(("product", products));
        files.extend(described);

        let summary: Vec<String> = counts
            .iter()
            .filter(|&&(_, count)| count > 0)
            .map(|&(kind, count)| {
                if count == 1 {
                    kind.to_owned()
                } else {
                    format!("{kind} ({count})")
                }
            })
            .collect();
        let message = format!("{name}: {}", summary.join(", "));
        drop(tri!(app.save(&files, &message).await));
        let today = self.now.date_naive();
        let next_from = tri!(
            today
                .checked_sub_signed(Duration::days(OVERLAP_DAYS))
                .context("history cursor")
        );
        tri!(app.store.set(&self.source.history_key(), &next_from));
        if !unseen.is_empty() {
            let mut known = seen;
            known.extend(unseen);
            tri!(app.store.set(&self.source.products_key(), &known));
        }
        info!(
            "{name}: {} files, {} windows from {from}, {} new products",
            files.len(),
            windows.len(),
            products
        );
        Ok(())
    }

    /// One report over every window as files, one per window with items, with the product ids they mention added
    /// to `product_ids`.
    async fn report(
        &self,
        spec: &Report,
        windows: &[(NaiveDate, NaiveDate)],
        product_ids: &mut BTreeSet<String>,
    ) -> Result<Vec<File>> {
        let mut files = Vec::new();
        for &(start, end) in windows {
            let from_date = start.format("%d/%m/%Y").to_string();
            let to_date = end.format("%d/%m/%Y").to_string();
            let mut params = vec![
                ("fromDate", from_date.as_str()),
                ("toDate", to_date.as_str()),
            ];
            params.extend(spec.extra);
            let url = format!("{}/{}", self.base, spec.endpoint);
            let text = tri!(self.get(spec.endpoint, &url, &params).await);
            let reply: Value = tri!(serde_json::from_str(&text).context(spec.label));
            let Some(items) = reply.pointer(spec.items).and_then(Value::as_array) else {
                continue;
            };
            if items.is_empty() {
                continue;
            }
            // A product id as DEGIRO sends it, a number or a numeric string, as a string.
            let ids = items.iter().filter_map(|item| item.get("productId"));
            product_ids.extend(ids.filter_map(|id| {
                let numeric = id.as_number().map(ToString::to_string);
                numeric.or_else(|| {
                    id.as_str()
                        .filter(|candidate| !candidate.is_empty())
                        .map(str::to_owned)
                })
            }));
            let suffix = start.format("%Y-%m-%d").to_string();
            let request =
                json!({"endpoint": spec.endpoint, "fromDate": from_date, "toDate": to_date});
            files.push(tri!(self.stored(spec.kind, Some(&suffix), request, &text)));
        }
        Ok(files)
    }

    /// The session as query parameters. Before the client details are read the account is unknown, and the client
    /// details call answers 404 to any `intAccount`, so it is left out then.
    fn session_params(&self) -> Vec<(&str, String)> {
        let mut params = vec![("sessionId", self.session_id.clone())];
        if self.int_account != 0 {
            params.insert(0, ("intAccount", self.int_account.to_string()));
        }
        params
    }

    /// A file under `raw/<account>/<kind>/` holding the reply in the storage envelope.
    fn stored(&self, kind: &str, suffix: Option<&str>, request: Value, body: &str) -> Result<File> {
        let content = tri!(storage::envelope(self.now, request, body));
        Ok(File {
            path: storage::path(self.source.name, kind, self.now, suffix, "json"),
            content,
        })
    }
}

/// One DEGIRO account.
pub(super) struct Degiro {
    /// Directory under `raw/` and prefix of the store keys.
    name: &'static str,
}

impl Degiro {
    /// Store key of the date the next run fetches transactions and cash movements from.
    fn history_key(&self) -> String {
        format!("{}/history_from", self.name)
    }

    /// Store key of the product ids already described in `product`.
    fn products_key(&self) -> String {
        format!("{}/products_seen", self.name)
    }
}

#[async_trait]
impl Source for Degiro {
    async fn daily(&self, app: &App) -> Result<()> {
        let configured = if self.name == "degiro" {
            app.cfg.degiro.as_ref()
        } else {
            app.cfg.degiro_pension.as_ref()
        };
        let Some(login) = configured else {
            return Ok(());
        };
        let api = tri!(login.session(app, self).await);
        let result = api.pull().await;
        if let Err(error) = api.logout().await {
            warn!("{}: logout: {error:#}", self.name);
        }
        result
    }

    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// A web trader login. The TOTP secret is the base32 authenticator secret, needed with two-factor authentication on.
#[derive(Clone, Debug)]
pub(crate) struct Login {
    /// The account password.
    pub password: String,
    /// The base32 secret of the authenticator app, when two-factor authentication is on.
    pub totp_secret: Option<String>,
    /// The account username.
    pub username: String,
}

impl Login {
    /// The six-digit RFC 6238 one-time password for `unix` seconds, when a TOTP secret is set.
    fn one_time_password(&self, unix: u64) -> Result<Option<String>> {
        /// RFC 4648 base32, decoded in either case, ignoring spaces, dashes and padding.
        const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let Some(secret) = self.totp_secret.as_deref() else {
            return Ok(None);
        };
        let mut key = Vec::new();
        let mut buffer: u64 = 0;
        let mut bits = 0_u32;
        for letter in secret.bytes() {
            if letter == b' ' || letter == b'-' || letter == b'=' {
                continue;
            }
            let index = tri!(
                ALPHABET
                    .iter()
                    .position(|candidate| *candidate == letter.to_ascii_uppercase())
                    .with_context(|| format!(
                        "totp secret has a character outside base32: {}",
                        char::from(letter)
                    ))
            );
            buffer = (buffer << 5_u32) | tri!(u64::try_from(index));
            bits = bits.saturating_add(5);
            if bits >= 8 {
                bits = bits.saturating_sub(8);
                key.push(tri!(u8::try_from((buffer >> bits) & 0xff)));
                buffer &= (1_u64 << bits).saturating_sub(1);
            }
        }
        if key.is_empty() {
            bail!("totp secret is empty");
        }
        let mut mac = tri!(Hmac::<Sha1>::new_from_slice(&key).context("totp key"));
        // The step counter goes in big-endian; the big-endian conversions are linted against, so reverse the bytes.
        let mut counter = unix.checked_div(30).unwrap_or_default().to_le_bytes();
        counter.reverse();
        mac.update(&counter);
        let digest = mac.finalize().into_bytes();
        let offset = usize::from(digest.last().copied().unwrap_or_default() & 0x0f);
        let Some(&[first, second, third, fourth]) = digest.get(offset..offset.saturating_add(4))
        else {
            bail!("totp digest is shorter than 20 bytes");
        };
        let code = u32::from_le_bytes([fourth, third, second, first & 0x7f])
            .checked_rem(1_000_000)
            .unwrap_or_default();
        Ok(Some(format!("{code:06}")))
    }

    /// Log in, with a one-time password when a TOTP secret is set, and read the account number.
    async fn session<'app>(&self, app: &'app App, source: &'app Degiro) -> Result<Api<'app>> {
        let base = app.cfg.degiro_url.as_str();
        let mut body = json!({
            "username": self.username,
            "password": self.password,
            "isPassCodeReset": false,
            "isRedirectToMobile": false,
        });
        let unix = tri!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();
        let one_time_password = tri!(self.one_time_password(unix));
        let url = if one_time_password.is_some() {
            format!("{base}/login/secure/login/totp")
        } else {
            format!("{base}/login/secure/login")
        };
        if let Some(fields) = body.as_object_mut() {
            fields.extend(
                one_time_password.map(|code| ("oneTimePassword".to_owned(), Value::String(code))),
            );
        }
        let response = tri!(
            app.http
                .post(&url)
                .headers(headers(base))
                .json(&body)
                .send()
                .await
                .map_err(|error| anyhow!("degiro login: {}", error.without_url()))
        );
        let status = response.status();
        let text = tri!(
            response
                .text()
                .await
                .map_err(|error| anyhow!("degiro login: {}", error.without_url()))
        );
        let reply: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let status_text = reply
            .get("statusText")
            .and_then(Value::as_str)
            .unwrap_or("");
        let code = reply.get("status").and_then(Value::as_i64);
        if !status.is_success() {
            let hint = if reply.get("captchaRequired").and_then(Value::as_bool) == Some(true) {
                "DEGIRO asks for a captcha: log in once in a browser"
            } else {
                match code {
                    Some(6) => "two-factor authentication is on: set DEGIRO_TOTP_SECRET",
                    Some(12) => "DEGIRO wants the login approved in its app",
                    Some(3) => "bad credentials",
                    _ => "",
                }
            };
            bail!("degiro login {status}: {status_text} {hint}");
        }
        let Some(session_id) = reply
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            // Without two-step verification DEGIRO answers 202 and waits for a tap in its app.
            let hint = if status_text == "inAppTOTPNeeded" || code == Some(12) {
                ": DEGIRO wants each login approved in its app; turn on two-step verification with an \
                 authenticator app and set the TOTP secret"
            } else {
                ""
            };
            // Keys and the status text only: the values may hold secrets.
            let keys = reply
                .as_object()
                .map_or_default(|fields| fields.keys().cloned().collect::<Vec<_>>().join(","));
            bail!(
                "degiro login {status}: no sessionId in the reply (keys: [{keys}], statusText: {status_text}){hint}"
            );
        };
        let mut api = Api {
            app,
            base,
            int_account: 0,
            now: Utc::now(),
            session_id,
            source,
        };
        let client = tri!(
            api.get("pa/secure/client", &format!("{base}/pa/secure/client"), &[])
                .await
        );
        let details: Value = tri!(serde_json::from_str(&client).context("degiro client details"));
        api.int_account = tri!(
            details
                .pointer("/data/intAccount")
                .and_then(Value::as_i64)
                .context("degiro client details: no intAccount")
        );
        Ok(api)
    }
}

/// A report over a date window.
struct Report {
    /// Path under the base URL, which also names the call in errors.
    endpoint: &'static str,
    /// Extra query parameters.
    extra: &'static [(&'static str, &'static str)],
    /// JSON pointer to the list of items in the reply.
    items: &'static str,
    /// Directory under `raw/<account>/`.
    kind: &'static str,
    /// The reply in parse errors.
    label: &'static str,
}

/// Headers of the web trader; DEGIRO refuses requests that do not look like it.
fn headers(base: &str) -> HeaderMap {
    let mut all = HeaderMap::from_iter([
        (
            USER_AGENT,
            HeaderValue::from_static(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) \
                 Chrome/130.0.0.0 Safari/537.36",
            ),
        ),
        (
            ACCEPT,
            HeaderValue::from_static("application/json, text/plain, */*"),
        ),
    ]);
    all.extend(
        HeaderValue::from_str(base)
            .ok()
            .map(|origin| (ORIGIN, origin)),
    );
    all.extend(
        HeaderValue::from_str(&format!("{base}/trader"))
            .ok()
            .map(|referer| (REFERER, referer)),
    );
    all
}

/// Send and read the body. Errors name the call, never the URL, which carries the session id.
async fn send(what: &str, request: RequestBuilder) -> Result<String> {
    let response = tri!(
        request
            .send()
            .await
            .map_err(|error| anyhow!("degiro {what}: {}", error.without_url()))
    );
    let status = response.status();
    let text = tri!(
        response
            .text()
            .await
            .map_err(|error| anyhow!("degiro {what}: {}", error.without_url()))
    );
    if !status.is_success() {
        bail!(
            "degiro {what} {status}: {}",
            text.chars().take(300).collect::<String>()
        );
    }
    Ok(text)
}
