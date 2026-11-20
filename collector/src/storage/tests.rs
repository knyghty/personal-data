//! Tests for paths, envelopes, windows and slugs.

use anyhow::{Result, ensure};
use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};

use super::{envelope, path, slug, windows};
use crate::tri;

/// Parse an RFC 3339 timestamp.
fn parse(text: &str) -> Result<DateTime<Utc>> {
    Ok(tri!(DateTime::parse_from_rfc3339(text)).with_timezone(&Utc))
}

/// Paths nest by day and carry the stamp and the optional suffix.
#[test]
fn builds_paths() -> Result<()> {
    let at = tri!(parse("2026-10-13T09:00:00Z"));
    ensure!(
        path("withings", "measure", at, None, "json")
            == "raw/withings/measure/2026/10/13/20261013T090000.000Z.json",
        "path without suffix"
    );
    ensure!(
        path("withings", "measure", at, Some("2"), "json")
            == "raw/withings/measure/2026/10/13/20261013T090000.000Z-2.json",
        "path with suffix"
    );
    Ok(())
}

/// The response body is stored byte for byte, and a non-JSON body is an error.
#[test]
fn envelope_keeps_body_verbatim() -> Result<()> {
    let at = tri!(parse("2026-10-13T09:00:00Z"));
    let out = tri!(envelope(
        at,
        json!({"a": 1_u32}),
        r#"{"x": 1.10, "y": [ ]}"#
    ));
    ensure!(
        out.ends_with(r#""response":{"x": 1.10, "y": [ ]}}"#),
        "body kept verbatim: {out}"
    );
    ensure!(
        envelope(at, Value::Null, "not json").is_err(),
        "a non-JSON body is rejected"
    );
    Ok(())
}

/// Windows cover the range in steps, the last one shorter, capped in number, and empty for an empty range.
#[test]
fn slices_windows() -> Result<()> {
    let hourly = windows(
        tri!(parse("2026-10-13T00:00:00Z")),
        tri!(parse("2026-10-13T02:30:00Z")),
        Duration::hours(1),
        10,
    );
    ensure!(hourly.len() == 3, "two full hours and a half");
    let last = (
        tri!(parse("2026-10-13T02:00:00Z")),
        tri!(parse("2026-10-13T02:30:00Z")),
    );
    ensure!(hourly.get(2) == Some(&last), "the last window is shorter");
    let capped = windows(
        tri!(parse("2026-10-13T00:00:00Z")),
        tri!(parse("2026-10-14T00:00:00Z")),
        Duration::hours(1),
        5,
    );
    ensure!(capped.len() == 5, "the cap limits the windows");
    let none = windows(
        tri!(parse("2026-10-13T00:00:00Z")),
        tri!(parse("2026-10-13T00:00:00Z")),
        Duration::hours(1),
        5,
    );
    ensure!(none.is_empty(), "an empty range has no windows");
    Ok(())
}

/// Names lower-case to dashed ASCII, with separators collapsed and trimmed.
#[test]
fn slugs_names() -> Result<()> {
    ensure!(
        slug("Heart Rate (raw)") == "heart-rate-raw",
        "spaces and brackets become dashes"
    );
    ensure!(
        slug("  --Workouts  ") == "workouts",
        "leading and trailing separators go"
    );
    ensure!(slug("!!!").is_empty(), "only separators give an empty slug");
    Ok(())
}
