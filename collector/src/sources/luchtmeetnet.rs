//! Pull measured air quality from Luchtmeetnet (RIVM) stations near home, plus a one-off backfill to 2019.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::fmt::{self, Display, Formatter};

use anyhow::{Context as _, Error, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, SecondsFormat, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tracing::info;
use url::Url;

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Set when the backfill reached today. Remove it through `PUT /admin/state` to backfill again.
const BACKFILL_DONE: &str = "luchtmeetnet/backfill_done";
/// The first day to backfill.
const BACKFILL_FROM: NaiveDate = NaiveDate::from_ymd_opt(2019, 1, 1).unwrap();
/// The next day to backfill. Starts at `BACKFILL_FROM` and is removed when done.
const BACKFILL_KEY: &str = "luchtmeetnet/backfill_from";
/// Weeks per run. The API serves at most seven days per request.
const BACKFILL_WEEKS: usize = 26;
/// Hours per hourly pull. An hour appears about half an hour after it ends, so this covers a missed run.
const PAST_HOURS: i64 = 3;
/// File type and API path of each hourly series. Measurements come in pages of 1000, LKI values in pages of 100.
const SERIES: [(&str, &str); 2] = [("measurement", "measurements"), ("lki", "lki")];
/// Directory name under `raw/`.
const SOURCE: &str = "luchtmeetnet";

/// The Luchtmeetnet source.
pub(super) struct Luchtmeetnet;

/// The API answered 429. The backfill stops on it and carries on later. Raise it with `anyhow::Error::msg`.
#[derive(Debug)]
pub(super) struct RateLimited(pub String);

impl Display for RateLimited {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[async_trait]
impl Source for Luchtmeetnet {
    async fn daily(&self, app: &App) -> Result<()> {
        let now = Utc::now();
        let mut files = Vec::new();
        let mut hashes = Vec::new();
        for station in &app.cfg.luchtmeetnet_stations {
            let url = tri!(Url::parse(&format!(
                "{}/stations/{station}/",
                app.cfg.luchtmeetnet_url
            )));
            let text = tri!(get(app, &url).await);
            let key = format!("luchtmeetnet/sha/station/{station}");
            if let Some(sha) = steam::changed(app, &key, &text) {
                files.push(tri!(file("station", station, now, &url, &text)));
                hashes.push((key, sha));
            }
        }
        let message = format!("luchtmeetnet: daily ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));
        for (key, sha) in hashes {
            tri!(app.store.set(&key, &sha));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        if app.cfg.luchtmeetnet_stations.is_empty() {
            info!("luchtmeetnet: no stations, skipping");
            return Ok(());
        }
        let now = Utc::now();
        let start = tri!(
            now.checked_sub_signed(Duration::hours(PAST_HOURS))
                .context("start of the pull")
        );
        let mut files = Vec::new();
        for station in &app.cfg.luchtmeetnet_stations {
            for (kind, path) in SERIES {
                files.extend(tri!(pull(app, kind, path, station, start, now, now).await));
            }
        }
        let message = format!("luchtmeetnet: hourly ({} files)", files.len());
        drop(tri!(app.save(&files, &message).await));

        // Backfill up to `BACKFILL_WEEKS` weeks of every series for every station, one file per page named by the
        // week's start. Each week is committed and the cursor saved on its own, and a 429 ends the run so the next
        // one resumes there.
        if app.store.get::<bool>(BACKFILL_DONE) == Some(true) {
            return Ok(());
        }
        let mut cursor = app.store.get(BACKFILL_KEY).unwrap_or(BACKFILL_FROM);
        for _ in 0..BACKFILL_WEEKS {
            let from = cursor.and_time(NaiveTime::MIN).and_utc();
            if from >= now {
                break;
            }
            let to = tri!(
                from.checked_add_signed(Duration::days(7))
                    .context("end of the week")
            )
            .min(now);
            let mut week = Vec::new();
            for station in &app.cfg.luchtmeetnet_stations {
                for (kind, path) in SERIES {
                    let pulled = pull(app, kind, path, station, from, to, from).await;
                    match pulled {
                        Ok(pages) => week.extend(pages),
                        Err(error) if error.is::<RateLimited>() => {
                            info!(
                                "luchtmeetnet: backfill rate limited at {cursor}, resuming next run"
                            );
                            return Ok(());
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            cursor = tri!(
                cursor
                    .checked_add_signed(Duration::days(7))
                    .context("next week")
            );
            if !week.is_empty() {
                let week_message = format!("luchtmeetnet: backfill to {cursor}");
                drop(tri!(app.save(&week, &week_message).await));
            }
            tri!(app.store.set(BACKFILL_KEY, &cursor));
        }
        if cursor.and_time(NaiveTime::MIN).and_utc() >= now {
            info!("luchtmeetnet: backfill done");
            tri!(app.store.set(BACKFILL_DONE, &true));
            tri!(app.store.remove(BACKFILL_KEY));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// Wrap a reply as a file of `kind` named by `at` and `suffix`.
fn file(kind: &str, suffix: &str, at: DateTime<Utc>, url: &Url, body: &str) -> Result<File> {
    Ok(File {
        path: storage::path(SOURCE, kind, at, Some(suffix), "json"),
        content: tri!(storage::envelope(
            Utc::now(),
            json!({"url": url.as_str()}),
            body
        )),
    })
}

/// GET a URL; a redirect or a non-2xx reply is an error, a 429 a `RateLimited` one.
async fn get(app: &App, url: &Url) -> Result<String> {
    let resp = tri!(app.http.get(url.clone()).send().await);
    // A range over seven days is redirected to its last seven days, which would file data under the wrong window.
    if resp.url() != url {
        bail!("luchtmeetnet redirected {url} to {}", resp.url());
    }
    let status = resp.status();
    let text = tri!(resp.text().await);
    if !status.is_success() {
        let snippet: String = text.chars().take(300).collect();
        let message = format!("luchtmeetnet {url} {status}: {snippet}");
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::msg(RateLimited(message)));
        }
        bail!(message);
    }
    Ok(text)
}

/// GET every page of one series of `station` from `start` to `end`, one file per page named by `at`.
async fn pull(
    app: &App,
    kind: &str,
    path: &str,
    station: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    at: DateTime<Utc>,
) -> Result<Vec<File>> {
    let mut files = Vec::new();
    let mut page = 1_u64;
    loop {
        let url = tri!(Url::parse_with_params(
            &format!("{}/{path}", app.cfg.luchtmeetnet_url),
            [
                ("station_number", station.to_owned()),
                ("start", start.to_rfc3339_opts(SecondsFormat::Secs, true)),
                ("end", end.to_rfc3339_opts(SecondsFormat::Secs, true)),
                ("page", page.to_string()),
            ],
        ));
        let text = tri!(get(app, &url).await);
        let reply: Value = tri!(serde_json::from_str(&text));
        let last = reply
            .pointer("/pagination/last_page")
            .and_then(Value::as_u64)
            .unwrap_or(1);
        let suffix = if page == 1 {
            station.to_owned()
        } else {
            format!("{station}-{page}")
        };
        files.push(tri!(file(kind, &suffix, at, &url, &text)));
        if page >= last {
            return Ok(files);
        }
        page = page.saturating_add(1);
    }
}
