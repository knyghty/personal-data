//! Build append-only paths, wrap responses in an envelope and slice time windows.

#[cfg(test)]
mod tests;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Duration, Timelike as _, Utc};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::tri;

/// A file to commit: repo-relative path and text content.
#[derive(Debug, Clone)]
pub(crate) struct File {
    /// The text to store.
    pub content: String,
    /// Repo-relative path.
    pub path: String,
}

/// Count the entries of a JSON array or object; anything else counts as zero.
pub(crate) fn count(value: &Value) -> usize {
    value
        .as_array()
        .map(Vec::len)
        .or_else(|| value.as_object().map(Map::len))
        .unwrap_or(0)
}

/// Wrap a raw JSON response body with fetch metadata. The body is kept verbatim.
///
/// # Errors
/// The body is not JSON.
pub(crate) fn envelope(fetched_at: DateTime<Utc>, request: Value, body: &str) -> Result<String> {
    /// The stored shape.
    #[derive(Serialize)]
    struct Envelope<'body> {
        /// When the response arrived.
        fetched_at: DateTime<Utc>,
        /// What was asked for.
        request: Value,
        /// The response body, verbatim.
        response: &'body RawValue,
    }
    let response: &RawValue = tri!(serde_json::from_str(body).context("response body is not JSON"));
    Ok(tri!(serde_json::to_string(&Envelope {
        fetched_at,
        request,
        response,
    })))
}

/// Truncate to the start of the hour.
pub(crate) fn floor_hour(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_minute(0)
        .and_then(|hour| hour.with_second(0))
        .and_then(|hour| hour.with_nanosecond(0))
        .unwrap_or(at)
}

/// Build `raw/<source>/<kind>/YYYY/MM/DD/<stamp>[-suffix].<ext>`, the stamp being the UTC time like
/// `20261013T090000.000Z`.
pub(crate) fn path(
    source: &str,
    kind: &str,
    at: DateTime<Utc>,
    suffix: Option<&str>,
    ext: &str,
) -> String {
    let mut name = at.format("%Y%m%dT%H%M%S%.3fZ").to_string();
    if let Some(tail) = suffix {
        name.push('-');
        name.push_str(tail);
    }
    format!("raw/{source}/{kind}/{}/{name}.{ext}", at.format("%Y/%m/%d"))
}

/// Lower-case a free-form name into a path segment of `[a-z0-9-]`.
pub(crate) fn slug(name: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            out.push(character.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        } else {
            // Collapse runs of separators and drop leading ones.
        }
    }
    out.trim_end_matches('-').to_owned()
}

/// Split `[start, end)` into consecutive windows of `step`, at most `cap` of them.
/// The last window is shorter when the range is not a multiple of `step`.
pub(crate) fn windows(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: Duration,
    cap: usize,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let mut out = Vec::new();
    let mut from = start;
    while from < end && out.len() < cap {
        let to = from
            .checked_add_signed(step)
            .map_or(end, |next| next.min(end));
        out.push((from, to));
        from = to;
    }
    out
}
