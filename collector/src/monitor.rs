//! Report errors and the hourly run to Sentry, and start the fixer routine. Each part is a no-op when its secret is unset.

#[cfg(test)]
mod tests;

use core::str::FromStr;

use anyhow::{Context as _, Error, Result, bail};
use reqwest::Client;
use serde_json::{Value, json};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::tri;

/// Slug of the one cron monitor on the free plan.
const MONITOR: &str = "hourly-run";

/// Cron check-ins for the hourly run, parsed from the Sentry DSN. Sentry opens an issue when a check-in is missed, late
/// or failed. The default sends nothing.
#[derive(Default)]
pub(crate) struct Cron {
    /// The check-in URL, or `None` without a DSN.
    url: Option<String>,
}

/// A Claude routine with an API trigger. `fire` starts one cloud session with `text` as its payload.
pub(crate) struct Routine {
    /// Shared HTTP client.
    pub http: Client,
    /// Bearer token of the trigger.
    pub token: Option<String>,
    /// The trigger URL.
    pub url: Option<String>,
}

impl Cron {
    /// Report the end of a run.
    pub(crate) async fn finish(&self, http: &Client, id: &str, ok: bool) {
        let status = if ok { "ok" } else { "error" };
        self.send(http, json!({"check_in_id": id, "status": status}))
            .await;
    }

    /// Post one check-in, logging instead of failing.
    async fn send(&self, http: &Client, body: Value) {
        let Some(url) = self.url.as_deref() else {
            return;
        };
        match http.post(url).json(&body).send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => warn!("sentry: check-in returned {}", response.status()),
            Err(error) => warn!("sentry: check-in failed: {error:#}"),
        }
    }

    /// Report the start of a run. Also creates or updates the monitor itself.
    pub(crate) async fn start(&self, http: &Client) -> String {
        let id = Uuid::new_v4().to_string();
        self.send(
            http,
            json!({
                "check_in_id": id,
                "status": "in_progress",
                "monitor_config": {
                    "schedule": {"type": "interval", "value": 1_u32, "unit": "hour"},
                    "checkin_margin": 30_u32,
                    "max_runtime": 55_u32,
                    "failure_issue_threshold": 1_u32,
                    "recovery_threshold": 1_u32,
                    "timezone": "UTC",
                },
            }),
        )
        .await;
        id
    }
}

impl FromStr for Cron {
    type Err = Error;

    /// Derive the check-in URL from the DSN `https://<key>@<host>/<project>`.
    fn from_str(dsn: &str) -> Result<Self> {
        let parsed = tri!(Url::parse(dsn).context("parse SENTRY_DSN"));
        let host = tri!(parsed.host_str().context("SENTRY_DSN has no host"));
        let project = parsed.path().trim_matches('/');
        Ok(Self {
            url: Some(format!(
                "{}://{host}/api/{project}/cron/{MONITOR}/{}/",
                parsed.scheme(),
                parsed.username()
            )),
        })
    }
}

impl Routine {
    /// Whether both the trigger URL and its token are set.
    pub(crate) const fn configured(&self) -> bool {
        self.url.is_some() && self.token.is_some()
    }

    /// Start a run. Returns the session URL.
    ///
    /// # Errors
    /// The routine is not configured or the trigger request fails.
    pub(crate) async fn fire(&self, text: &str) -> Result<String> {
        let (Some(url), Some(token)) = (self.url.as_deref(), self.token.as_deref()) else {
            bail!("routine not configured");
        };
        let response = tri!(
            self.http
                .post(url)
                .bearer_auth(token)
                .header("anthropic-beta", "experimental-cc-routine-2026-04-01")
                .header("anthropic-version", "2023-06-01")
                .json(&json!({"text": text}))
                .send()
                .await
                .context("fire routine")
        );
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            bail!("fire routine: {status} {body}");
        }
        let session = body
            .get("claude_code_session_url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        info!(session, "routine fired");
        Ok(session)
    }
}
