//! The owner's bookkeeping in Moneybird: invoices, receipts, journal entries, contacts, bank mutations and time entries.
//!
//! The REST API v2 is read with a read-only personal access token (`MONEYBIRD_TOKEN`). Every administration the token
//! can see is pulled. The large lists have a synchronization endpoint: `GET .../synchronization` returns the id and
//! version of every item. The collector keeps `id -> version` per resource in the state and fetches only new or changed
//! items, one `GET .../{id}` each, so an edited invoice is stored again. The batch fetch, `POST .../synchronization`,
//! would take 100 ids per request, but a read-only token refuses every POST. Time entries have no synchronization, so
//! the whole period is paged and compared on `updated_at`. The daily job stores the administrations, ledger accounts,
//! projects, products, tax rates, financial accounts and recurring invoices, each only when changed.
//!
//! Moneybird allows 150 requests per 5 minutes. A run makes at most `REQUESTS_PER_RUN` and a 429 ends it; what was
//! committed stays, and the next run carries on from the state. Versions move only after a successful commit.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::sync::Arc;
use core::fmt::{self, Display, Formatter};
use core::slice;

use anyhow::{Context as _, Error, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Datelike as _, NaiveDate, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tracing::info;
use url::Url;

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

#[cfg(test)]
mod tests;

/// The lists the daily job stores.
const DAILY: [Daily; 6] = [
    Daily {
        kind: "ledger_account",
        paged: false,
        path: "ledger_accounts",
        query: &[],
    },
    Daily {
        kind: "project",
        paged: true,
        path: "projects",
        query: &[("filter", "state:all")],
    },
    Daily {
        kind: "product",
        paged: true,
        path: "products",
        query: &[],
    },
    Daily {
        kind: "tax_rate",
        paged: true,
        path: "tax_rates",
        query: &[],
    },
    Daily {
        kind: "financial_account",
        paged: false,
        path: "financial_accounts",
        query: &[],
    },
    Daily {
        kind: "recurring_sales_invoice",
        paged: true,
        path: "recurring_sales_invoices",
        query: &[],
    },
];
/// Items per page of a paged list, the API's maximum.
const PER_PAGE: usize = 100;
/// Requests per run, under the limit of 150 per 5 minutes with room for the daily lists. Each changed item costs one,
/// so the first runs fetch about this many items an hour.
const REQUESTS_PER_RUN: usize = 120;
/// Directory under `raw/`.
const SOURCE: &str = "moneybird";
/// The resources the hourly job synchronizes.
const SYNCED: [Synced; 6] = [
    Synced {
        kind: "sales_invoice",
        path: "sales_invoices",
        query: &[("filter", "period:PERIOD,state:all")],
    },
    Synced {
        kind: "purchase_invoice",
        path: "documents/purchase_invoices",
        query: &[("filter", "period:PERIOD,state:all")],
    },
    Synced {
        kind: "receipt",
        path: "documents/receipts",
        query: &[("filter", "period:PERIOD,state:all")],
    },
    Synced {
        kind: "journal_document",
        path: "documents/general_journal_documents",
        query: &[("filter", "period:PERIOD,state:all")],
    },
    Synced {
        kind: "contact",
        path: "contacts",
        query: &[("include_archived", "true")],
    },
    Synced {
        kind: "financial_mutation",
        path: "financial_mutations",
        query: &[("filter", "period:PERIOD,state:all,mutation_type:all")],
    },
];
/// A `period` filter may span at most 10 years, so pull from this many years back.
const YEARS: i32 = 9;

/// A session over the API with the request budget of one run.
struct Client<'app> {
    /// HTTP, the store and GitHub.
    app: &'app App,
    /// Requests left in this run.
    left: usize,
    /// When the run started; the files are stamped with it.
    now: DateTime<Utc>,
    /// The personal access token.
    token: &'app str,
}

impl<'app> Client<'app> {
    /// GET `path` under the API root. A 429 fails with `RateLimited`; any other failure fails the run.
    async fn get(&mut self, path: &str, query: &[(&str, &str)]) -> Result<String> {
        self.left = self.left.saturating_sub(1);
        let url = tri!(Url::parse_with_params(
            &format!("{}/{path}", self.app.cfg.moneybird_api_url),
            query,
        ));
        let response = tri!(
            self.app
                .http
                .get(url)
                .bearer_auth(self.token)
                .header("accept", "application/json")
                .send()
                .await
        );
        let status = response.status();
        let text = tri!(response.text().await);
        if !status.is_success() {
            let snippet: String = text.chars().take(300).collect();
            let message = format!("moneybird {path} {status}: {snippet}");
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(Error::msg(RateLimited(message)));
            }
            bail!(message);
        }
        Ok(text)
    }

    /// A session with a full request budget for a run starting at `now`.
    const fn new(app: &'app App, token: &'app str, now: DateTime<Utc>) -> Self {
        Self {
            app,
            left: REQUESTS_PER_RUN,
            now,
            token,
        }
    }

    /// Every page of a list as one JSON array, with the number of pages read; one GET when the list is not paged.
    async fn pages(
        &mut self,
        path: &str,
        query: &[(&str, &str)],
        paged: bool,
    ) -> Result<(String, usize)> {
        if !paged {
            let body = tri!(self.get(path, query).await);
            return Ok((body, 1));
        }
        let mut all = Vec::new();
        let mut page = 1_usize;
        loop {
            let per_page = PER_PAGE.to_string();
            let page_number = page.to_string();
            let mut page_query = query.to_vec();
            page_query.push(("per_page", per_page.as_str()));
            page_query.push(("page", page_number.as_str()));
            let body = tri!(self.get(path, &page_query).await);
            let items: Vec<Value> =
                tri!(serde_json::from_str(&body).with_context(|| path.to_owned()));
            let count = items.len();
            all.extend(items);
            if count < PER_PAGE {
                break;
            }
            page = page.saturating_add(1);
        }
        Ok((Value::Array(all).to_string(), page))
    }

    /// The `period` filter: from the administration's first bookkeeping year, at most `YEARS` back, until today.
    fn period(&self, admin: &Value) -> String {
        let today = self.now.date_naive();
        let floor = NaiveDate::from_ymd_opt(today.year().saturating_sub(YEARS), 1, 1)
            .unwrap_or(NaiveDate::MIN);
        let start = admin
            .get("period_start_date")
            .and_then(Value::as_str)
            .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
            .map_or(floor, |date| date.max(floor));
        format!("{}..{}", start.format("%Y%m%d"), today.format("%Y%m%d"))
    }

    /// Store the new and changed items of one synchronized resource. `Ok(false)` means the run ran out of requests
    /// before the resource was complete; the rest waits for the next run.
    async fn synced(&mut self, admin: &Value, resource: &Synced, period: &str) -> Result<bool> {
        if self.left == 0 {
            return Ok(false);
        }
        let id = tri!(admin_id(admin));
        let path = format!("{id}/{}/synchronization.json", resource.path);
        let filled: Vec<(&str, String)> = resource
            .query
            .iter()
            .map(|&(name, value)| (name, value.replace("PERIOD", period)))
            .collect();
        let query: Vec<(&str, &str)> = filled
            .iter()
            .map(|pair| (pair.0, pair.1.as_str()))
            .collect();
        let body = tri!(self.get(&path, &query).await);
        let listed: Vec<Value> = tri!(serde_json::from_str(&body).with_context(|| path.clone()));
        let key = versions_key(id, resource.kind);
        let known: BTreeMap<String, Value> = self.app.store.get(&key).unwrap_or_default();
        let mut current = BTreeMap::new();
        let mut changed = Vec::new();
        for item in &listed {
            let Some(item_id) = item.get("id").and_then(Value::as_str) else {
                continue;
            };
            let version = item.get("version").cloned().unwrap_or(Value::Null);
            if known.get(item_id) != Some(&version) {
                changed.push(item_id.to_owned());
            }
            drop(current.insert(item_id.to_owned(), version));
        }
        let mut items = Vec::new();
        let mut complete = true;
        for item_id in &changed {
            if self.left == 0 {
                complete = false;
                break;
            }
            let item_path = format!("{id}/{}/{item_id}.json", resource.path);
            let item = tri!(self.get(&item_path, &[]).await);
            items.push(tri!(
                serde_json::from_str::<Value>(&item).with_context(|| item_path)
            ));
        }
        let fetched = items.len();
        if fetched > 0 {
            let file = File {
                path: storage::path(SOURCE, resource.kind, self.now, Some(id), "json"),
                content: tri!(storage::envelope(
                    self.now,
                    json!({"administration": id, "path": resource.path, "ids": fetched}),
                    &Value::Array(items).to_string(),
                )),
            };
            let message = format!("moneybird: {} ({fetched})", resource.kind);
            drop(tri!(self.app.save(slice::from_ref(&file), &message).await));
        }
        // Items not fetched this run keep their old version, so the next run fetches them. Deleted items drop out.
        let pending: BTreeSet<&str> = changed.iter().skip(fetched).map(String::as_str).collect();
        let next: BTreeMap<&str, &Value> = current
            .iter()
            .filter_map(|(item_id, version)| {
                if pending.contains(item_id.as_str()) {
                    known.get(item_id).map(|old| (item_id.as_str(), old))
                } else {
                    Some((item_id.as_str(), version))
                }
            })
            .collect();
        tri!(self.app.store.set(&key, &next));
        Ok(complete)
    }

    /// Store the time entries of the period that are new or changed since the last run, as one list.
    async fn time_entries(&mut self, admin: &Value, period: &str) -> Result<bool> {
        if self.left == 0 {
            return Ok(false);
        }
        let id = tri!(admin_id(admin));
        let filter = format!("period:{period},state:all");
        let (body, _pages) = tri!(
            self.pages(
                &format!("{id}/time_entries.json"),
                &[("filter", &filter)],
                true
            )
            .await
        );
        let entries: Vec<Value> = tri!(serde_json::from_str(&body));
        let key = versions_key(id, "time_entry");
        let known: BTreeMap<String, Value> = self.app.store.get(&key).unwrap_or_default();
        let mut current = BTreeMap::new();
        let mut changed = Vec::new();
        for entry in entries {
            let Some(entry_id) = entry.get("id").and_then(Value::as_str) else {
                continue;
            };
            let version = entry.get("updated_at").cloned().unwrap_or(Value::Null);
            if known.get(entry_id) != Some(&version) {
                changed.push(entry.clone());
            }
            drop(current.insert(entry_id.to_owned(), version));
        }
        if !changed.is_empty() {
            let count = changed.len();
            let file = File {
                path: storage::path(SOURCE, "time_entry", self.now, Some(id), "json"),
                content: tri!(storage::envelope(
                    self.now,
                    json!({"administration": id, "path": "time_entries", "count": count}),
                    &Value::Array(changed).to_string(),
                )),
            };
            let message = format!("moneybird: time_entry ({count})");
            drop(tri!(self.app.save(slice::from_ref(&file), &message).await));
        }
        tri!(self.app.store.set(&key, &current));
        Ok(true)
    }
}

/// A list per administration that the daily job stores whole when it changed.
struct Daily {
    /// Directory under `raw/moneybird/`.
    kind: &'static str,
    /// Whether the endpoint takes `page` and `per_page`.
    paged: bool,
    /// Path under the administration.
    path: &'static str,
    /// Extra query parameters.
    query: &'static [(&'static str, &'static str)],
}

/// The owner's bookkeeping.
pub(super) struct Moneybird;

#[async_trait]
impl Source for Moneybird {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(token) = app.cfg.moneybird_token.as_deref() else {
            return Ok(());
        };
        let now = Utc::now();
        let mut client = Client::new(app, token, now);
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        let listed = tri!(client.get("administrations.json", &[]).await);
        let admin_key = "moneybird/sha/administration".to_owned();
        if let Some(sha) = steam::changed(app, &admin_key, &listed) {
            files.push(File {
                path: storage::path(SOURCE, "administration", now, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    json!({"path": "administrations"}),
                    &listed
                )),
            });
            hashes.push((admin_key, sha));
        }
        let admins: Vec<Value> = tri!(serde_json::from_str(&listed).context("administrations"));
        for admin in &admins {
            let id = tri!(admin_id(admin));
            for list in &DAILY {
                let path = format!("{id}/{}.json", list.path);
                let (body, pages) = tri!(client.pages(&path, list.query, list.paged).await);
                let key = format!("moneybird/sha/{id}/{}", list.kind);
                if let Some(sha) = steam::changed(app, &key, &body) {
                    files.push(File {
                        path: storage::path(SOURCE, list.kind, now, Some(id), "json"),
                        content: tri!(storage::envelope(
                            now,
                            json!({"administration": id, "path": path, "pages": pages}),
                            &body,
                        )),
                    });
                    hashes.push((key, sha));
                }
            }
        }
        let message = format!("moneybird: daily ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (key, sha) in hashes {
            tri!(app.store.set(&key, &sha));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(token) = app.cfg.moneybird_token.as_deref() else {
            return Ok(());
        };
        let mut client = Client::new(app, token, Utc::now());
        let listed = tri!(client.get("administrations.json", &[]).await);
        let admins: Vec<Value> = tri!(serde_json::from_str(&listed).context("administrations"));
        let outcome: Result<bool> = async {
            for admin in &admins {
                let period = client.period(admin);
                for resource in &SYNCED {
                    if !tri!(client.synced(admin, resource, &period).await) {
                        return Ok(false);
                    }
                }
                if !tri!(client.time_entries(admin, &period).await) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        .await;
        match outcome {
            Ok(true) => Ok(()),
            Ok(false) => {
                info!("moneybird: out of requests, resuming next run");
                Ok(())
            }
            Err(error) if error.is::<RateLimited>() => {
                info!("moneybird: rate limited, resuming next run");
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// The API answered 429. The run stops and carries on next time.
#[derive(Debug)]
struct RateLimited(String);

impl Display for RateLimited {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A resource with synchronization endpoints.
struct Synced {
    /// Directory under `raw/moneybird/`.
    kind: &'static str,
    /// Path under the administration.
    path: &'static str,
    /// Query of the `GET .../synchronization` call; `PERIOD` stands for the pulled range.
    query: &'static [(&'static str, &'static str)],
}

/// The administration's id, a numeric string.
fn admin_id(admin: &Value) -> Result<&str> {
    admin
        .get("id")
        .and_then(Value::as_str)
        .context("administration id")
}

/// Store key of the `id -> version` map of one resource of an administration.
fn versions_key(admin: &str, kind: &str) -> String {
    format!("moneybird/{admin}/{kind}/versions")
}
