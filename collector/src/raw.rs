//! Keep raw files in the bucket, gzipped, and tell the query server which ones arrived.

#[cfg(test)]
mod tests;

use core::time::Duration;
use std::io::Write as _;

use anyhow::{Result, bail};
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::{Client, StatusCode};
use rusty_s3::{Bucket, Credentials, S3Action as _};
use serde_json::json;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::storage::File;
use crate::tri;

/// Attempts per upload when the bucket answers 5xx or 429, or cannot be reached.
const ATTEMPTS: u32 = 3;
/// How long the query server may take to accept new files, its start included.
const INGEST_TIMEOUT: Duration = Duration::from_mins(1);
/// Wait after the first failed upload, in milliseconds; each further attempt waits a multiple.
const RETRY_PAUSE_MS: u64 = if cfg!(test) { 0 } else { 2000 };
/// How long a signed upload URL stays valid.
const SIGNED_FOR: Duration = Duration::from_mins(15);

/// The bucket that holds `raw/`, and the query server to tell about new files.
pub(crate) struct Raw {
    /// The bucket, path style.
    pub bucket: Bucket,
    /// Access key of the bucket.
    pub credentials: Credentials,
    /// Shared HTTP client.
    pub http: Client,
    /// The query server's ingest URL, secret token included. `None` skips the call.
    pub ingest_url: Option<String>,
}

impl Raw {
    /// The bucket key of a repo-relative path: the same path with `.gz` appended.
    pub(crate) fn key(path: &str) -> String {
        format!("{path}.gz")
    }

    /// Tell the query server about `paths`. It also lists the bucket, so a failed call only delays the tables.
    async fn notify(&self, paths: &[&str], message: &str) {
        let Some(url) = self.ingest_url.as_deref() else {
            return;
        };
        let sent = self
            .http
            .post(url)
            .timeout(INGEST_TIMEOUT)
            .json(&json!({"message": message, "paths": paths}))
            .send()
            .await;
        match sent {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => warn!(status = %response.status(), "ingest refused: {message}"),
            Err(error) => warn!("ingest failed: {message}: {error}"),
        }
    }

    /// Upload one file gzipped, retrying on 5xx, 429 and network errors.
    async fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        let key = Self::key(path);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        tri!(encoder.write_all(content));
        let body = tri!(encoder.finish());
        let mut attempt = 1_u32;
        loop {
            let url = self
                .bucket
                .put_object(Some(&self.credentials), &key)
                .sign(SIGNED_FOR);
            let sent = self
                .http
                .put(url)
                .header("content-type", "application/gzip")
                .body(body.clone())
                .send()
                .await;
            let reason = match sent {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    if !status.is_server_error() && status != StatusCode::TOO_MANY_REQUESTS {
                        bail!("upload {key} failed: {status} {text}");
                    }
                    format!("{status} {text}")
                }
                Err(error) => format!("{error}"),
            };
            if attempt >= ATTEMPTS {
                bail!("upload {key} failed: {reason}");
            }
            warn!(attempt, "upload {key} failed, retrying: {reason}");
            sleep(Duration::from_millis(RETRY_PAUSE_MS).saturating_mul(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Upload `files` and tell the query server. Returns the first file's key, or `None` when there is nothing to store.
    ///
    /// # Errors
    /// An upload fails for good.
    pub(crate) async fn save(&self, files: &[File], message: &str) -> Result<Option<String>> {
        let Some(first) = files.first() else {
            return Ok(None);
        };
        for file in files {
            tri!(self.put(&file.path, file.content.as_bytes()).await);
        }
        info!(files = files.len(), "stored: {message}");
        let paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();
        self.notify(&paths, message).await;
        Ok(Some(Self::key(&first.path)))
    }

    /// Store one file of any bytes at `path`, which must sit under `raw/`, and tell the query server. For files
    /// added by hand, such as lab reports and app exports. Returns the key.
    ///
    /// # Errors
    /// The path is outside `raw/`, or the upload fails for good.
    pub(crate) async fn upload(&self, path: &str, content: &[u8]) -> Result<String> {
        if !path.starts_with("raw/") || path.split('/').any(|part| part.is_empty() || part == "..")
        {
            bail!("{path} is not a path under raw/");
        }
        tri!(self.put(path, content).await);
        let message = format!("upload: {path} ({} bytes)", content.len());
        info!("stored: {message}");
        self.notify(&[path], &message).await;
        Ok(Self::key(path))
    }
}
