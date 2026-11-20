//! Tests for `flights`.

use std::env::temp_dir;
use std::fs;
use std::process;

use anyhow::{Result, ensure};
use serde_json::{Value, json};

use super::Flights;
use crate::tri;

/// Keep the polls of a push until the collector accepts it.
#[test]
fn buffers_polls_until_a_push_succeeds() -> Result<()> {
    let dir = temp_dir().join(format!("personal-data-mac-flights-{}", process::id()));
    drop(fs::remove_dir_all(&dir));
    tri!(fs::create_dir_all(&dir));
    let flights = Flights { support: &dir };
    ensure!(tri!(flights.take()).is_empty(), "nothing polled yet");

    tri!(flights.append(&json!({"ac": [{"flight": "KLM1"}]})));
    tri!(flights.append(&json!({"ac": []})));
    let first = tri!(flights.take());
    ensure!(first.len() == 2, "both polls are taken");
    let first_flight = |polls: &[Value]| {
        polls
            .first()
            .and_then(|poll| poll.pointer("/reply/ac/0/flight"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    ensure!(
        first_flight(&first).as_deref() == Some("KLM1"),
        "the first poll is the first line"
    );

    // A poll during the push goes to a new buffer; the push failed, so the same polls are sent again.
    tri!(flights.append(&json!({"ac": [{"flight": "KLM2"}]})));
    ensure!(tri!(flights.take()) == first, "a failed push is sent again");
    tri!(flights.sent());
    let next = tri!(flights.take());
    ensure!(next.len() == 1, "only the poll during the push is left");
    ensure!(
        first_flight(&next).as_deref() == Some("KLM2"),
        "the later poll is sent next"
    );
    tri!(flights.sent());
    ensure!(tri!(flights.take()).is_empty(), "nothing is left");
    Ok(())
}
