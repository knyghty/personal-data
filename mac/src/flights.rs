//! Aircraft near home, from adsb.lol's public API, polled every minute by a second launchd job (`--flights`).
//!
//! Each poll appends adsb.lol's reply, verbatim, as one line of `flights.jsonl`. The 15-minute run moves that file to
//! `flights.sending`, sends its lines with the push, and deletes it only after the collector accepted the push, so
//! polls during a push go to a new `flights.jsonl` and a failed push is sent again next time.

#[cfg(test)]
mod tests;

use core::time::Duration;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ureq::Agent;

use crate::tri;

/// adsb.lol's query for the aircraft around a point.
const API: &str = "https://api.adsb.lol/v2/point";

/// Where to look: a point and a radius in nautical miles.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(super) struct Area {
    /// Latitude of the point, in degrees.
    pub lat: f64,
    /// Longitude of the point, in degrees.
    pub lon: f64,
    /// Radius around the point, in nautical miles.
    pub radius_nm: f64,
}

/// The poll buffer in the support directory.
#[derive(Clone, Copy, Debug)]
pub(super) struct Flights<'support> {
    /// The directory that holds `flights.jsonl` and `flights.sending`.
    pub support: &'support Path,
}

impl Flights<'_> {
    /// Append `reply` to the buffer with the time of the poll.
    fn append(self, reply: &Value) -> Result<()> {
        let line = json!({"polled_at": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true), "reply": reply});
        let mut file = tri!(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.buffer())
        );
        tri!(writeln!(file, "{line}"));
        Ok(())
    }

    /// The file the polls go to.
    fn buffer(self) -> PathBuf {
        self.support.join("flights.jsonl")
    }

    /// Ask adsb.lol for the aircraft in `area` now and append the reply to the buffer.
    pub(crate) fn poll(self, area: &Area) -> Result<()> {
        let url = format!("{API}/{}/{}/{}", area.lat, area.lon, area.radius_nm);
        let agent: Agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(20)))
            .build()
            .into();
        let mut response = tri!(agent.get(&url).call().with_context(|| format!("GET {url}")));
        let text = tri!(response.body_mut().read_to_string());
        let reply: Value = tri!(serde_json::from_str(&text));
        self.append(&reply)
    }

    /// The file the polls of a push wait in.
    fn sending(self) -> PathBuf {
        self.support.join("flights.sending")
    }

    /// Forget the polls that were sent.
    pub(crate) fn sent(self) -> Result<()> {
        match fs::remove_file(self.sending()) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error.into()),
            Ok(()) | Err(_) => Ok(()),
        }
    }

    /// The polls to send: those left from a failed push, else the buffer, which is moved aside first.
    pub(crate) fn take(self) -> Result<Vec<Value>> {
        let pending = self.sending();
        if !pending.exists() {
            match fs::rename(self.buffer(), &pending) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(error.into()),
            }
        }
        let text = tri!(fs::read_to_string(&pending));
        // A line cut short by a crash is skipped rather than blocking every later push.
        let polls = text
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        Ok(polls)
    }
}
