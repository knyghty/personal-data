//! Read files from the code repo with the GitHub contents API, such as the offers watch list.

#[cfg(test)]
mod tests;

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use reqwest::Client;
use serde_json::Value;

use crate::tri;

/// A client for one repo and branch. Build it with the public fields and `..GitHub::default()`.
#[derive(Default)]
pub(crate) struct GitHub {
    /// API base, `https://api.github.com` in production, without a trailing slash.
    pub api: String,
    /// Branch to read files from.
    pub branch: String,
    /// Shared HTTP client.
    pub http: Client,
    /// The repo, as `owner/name`.
    pub repo: String,
    /// Personal access token. Empty for a public repo.
    pub token: String,
}

impl GitHub {
    /// The text of the file at the repo-relative `path` on the branch, from the contents API.
    ///
    /// # Errors
    /// The request fails, the file is missing, or its content is not base64 text.
    pub(crate) async fn file(&self, path: &str) -> Result<String> {
        let url = format!(
            "{}/repos/{}/contents/{path}?ref={}",
            self.api, self.repo, self.branch
        );
        let mut request = self
            .http
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        if !self.token.is_empty() {
            request = request.bearer_auth(&self.token);
        }
        let response = tri!(
            request
                .send()
                .await
                .with_context(|| format!("request {url}"))
        );
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("GET {path} failed: {status} {text}");
        }
        let reply: Value =
            tri!(serde_json::from_str(&text).with_context(|| format!("GET {path}: bad JSON")));
        let encoded: String = reply
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        let bytes = tri!(
            BASE64_STANDARD
                .decode(encoded)
                .with_context(|| format!("{path}: content is not base64"))
        );
        String::from_utf8(bytes).with_context(|| format!("{path}: content is not UTF-8"))
    }

    /// The repo, as `owner/name`.
    pub(crate) fn repo(&self) -> &str {
        &self.repo
    }
}
