//! Check how a notification is turned into a time window.

use std::collections::HashMap;

use anyhow::{Context as _, Result, ensure};
use chrono::{DateTime, Utc};

use super::Notification;
use crate::tri;

/// A notification with the given form fields.
fn notification(pairs: &[(&str, &str)]) -> Notification {
    let fields: HashMap<String, String> = pairs
        .iter()
        .map(|&(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    Notification(fields)
}

/// A `date` gives the day before to the day after it.
#[test]
fn notification_window_covers_the_day_around_date() -> Result<()> {
    let received = tri!(received());
    let midnight = 1_791_849_600_i64;
    let window = notification(&[("date", "2026-10-13")]).window(received);
    ensure!(
        window == (midnight - 86_400, midnight + 2 * 86_400),
        "unexpected window"
    );
    Ok(())
}

/// Without a range or a date the window is the 24 h before the notification arrived.
#[test]
fn notification_window_falls_back_to_last_day() -> Result<()> {
    let received = tri!(received());
    let window = notification(&[("appli", "1")]).window(received);
    ensure!(
        window == (1_800_000_000 - 86_400, 1_800_000_000),
        "unexpected window"
    );
    Ok(())
}

/// An explicit `startdate`/`enddate` wins over a `date`.
#[test]
fn notification_window_prefers_explicit_range() -> Result<()> {
    let received = tri!(received());
    let window = notification(&[
        ("startdate", "100"),
        ("enddate", "200"),
        ("date", "2026-10-13"),
    ])
    .window(received);
    ensure!(window == (100, 200), "unexpected window");
    Ok(())
}

/// The time the notifications in these tests arrived.
fn received() -> Result<DateTime<Utc>> {
    let at = tri!(DateTime::from_timestamp(1_800_000_000, 0).context("timestamp out of range"));
    Ok(at)
}
