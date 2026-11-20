//! Pull ICS calendars from their secret URLs, redacted where configured, and store what changed.
//!
//! A calendar's first file is whole: the calendar's own lines and time zones (`header`), every event parsed
//! (`events`) and its text (`vevents`), with `full: true`. Each later file holds only the events that are new or
//! changed, the `uid|recurrence-id` keys of the events that are gone (`removed`), and the header when it changed.
//! Event text drops `DTSTAMP`, which some servers set to the time of every download.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use core::mem;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tracing::info;

use super::{Source, steam};
use crate::app::App;
use crate::storage::{self, File};
use crate::tri;

/// Properties a redacted event keeps besides its hashed UID: times, recurrence and free/busy status.
const KEEP: &[&str] = &[
    "DTSTART",
    "DTEND",
    "DURATION",
    "RRULE",
    "RDATE",
    "EXDATE",
    "RECURRENCE-ID",
    "STATUS",
    "TRANSP",
    "X-MICROSOFT-CDO-BUSYSTATUS",
];
/// Directory name under `raw/`.
const SOURCE: &str = "calendar";
/// Properties copied into the parsed events besides `KEEP`.
const TEXT: &[&str] = &["UID", "SUMMARY", "LOCATION", "DESCRIPTION"];

/// The calendar source.
pub(super) struct Calendar;

/// One entry of `CALENDARS`. The URL is a credential: never log or commit it.
#[derive(Deserialize)]
struct Feed {
    /// Keep only times, status and hashed UIDs.
    #[serde(default)]
    redact: bool,
    /// The secret ICS URL.
    url: String,
}

/// An ICS document.
struct Ics<'text>(&'text str);

/// A content line split into its name, its parameters and its value.
struct Property<'line> {
    /// The upper-case property name.
    name: String,
    /// The parameters by upper-case name, unquoted.
    params: BTreeMap<String, String>,
    /// The value, escaped as in the document.
    value: &'line str,
}

impl Feed {
    /// Fetch the calendar. Returns its file when something changed since the last one, and the state to set once
    /// the file is stored.
    async fn snapshot(
        &self,
        app: &App,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<(Option<File>, Vec<(String, Value)>)> {
        let kind = storage::slug(name);
        if kind.is_empty() {
            bail!("calendar name {name:?} has no letters or digits");
        }
        let url = self.url.replacen("webcal://", "https://", 1);
        // Strip the URL from errors before they reach logs and issues.
        let resp = tri!(
            app.http
                .get(url)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
        );
        let status = resp.status();
        let text = tri!(resp.text().await.map_err(reqwest::Error::without_url));
        if !status.is_success() {
            bail!("calendar {name} {status}");
        }
        if !text.trim_start().starts_with("BEGIN:VCALENDAR") {
            bail!("calendar {name}: reply is not an ICS file");
        }
        let ics = if self.redact {
            Ics(&text).redact()
        } else {
            text
        };
        let key = format!("calendar/sha/{kind}");
        let document = Ics(&ics);
        let Some(sha) = steam::changed(app, &key, &document.fingerprint()) else {
            return Ok((None, Vec::new()));
        };
        let events = document.events();
        let (header, blocks) = document.split();
        let header_sha = format!("{:x}", Sha256::digest(&header));
        let (events_key, header_key) = (
            format!("calendar/events/{kind}"),
            format!("calendar/header/{kind}"),
        );
        let current: BTreeMap<String, String> = events
            .iter()
            .zip(&blocks)
            .map(|(event, block)| (event_key(event), format!("{:x}", Sha256::digest(block))))
            .collect();
        let previous: Option<BTreeMap<String, String>> = app.store.get(&events_key);
        let mut body = Map::new();
        match previous {
            // Whole: the first file of a calendar, or one whose event texts do not line up with its events.
            Some(old) if blocks.len() == events.len() => {
                let (changed, texts): (Vec<&Value>, Vec<&String>) = events
                    .iter()
                    .zip(&blocks)
                    .filter(|&(event, _)| {
                        let id = event_key(event);
                        old.get(&id) != current.get(&id)
                    })
                    .unzip();
                let removed: Vec<&String> =
                    old.keys().filter(|id| !current.contains_key(*id)).collect();
                let header_changed =
                    app.store.get::<String>(&header_key).as_deref() != Some(header_sha.as_str());
                if changed.is_empty() && removed.is_empty() && !header_changed {
                    return Ok((None, vec![(key, json!(sha))]));
                }
                drop(body.insert("full".into(), json!(false)));
                drop(body.insert("events".into(), json!(changed)));
                drop(body.insert("vevents".into(), json!(texts)));
                drop(body.insert("removed".into(), json!(removed)));
                if header_changed {
                    drop(body.insert("header".into(), json!(header)));
                }
            }
            _ => {
                drop(body.insert("full".into(), json!(true)));
                drop(body.insert("header".into(), json!(header)));
                drop(body.insert("events".into(), json!(events)));
                drop(body.insert("vevents".into(), json!(blocks)));
            }
        }
        let file = File {
            path: storage::path(SOURCE, &kind, now, None, "json"),
            content: tri!(storage::envelope(
                now,
                json!({"calendar": name, "redacted": self.redact}),
                &Value::Object(body).to_string(),
            )),
        };
        let state = vec![
            (key, json!(sha)),
            (events_key, json!(current)),
            (header_key, json!(header_sha)),
        ];
        Ok((Some(file), state))
    }
}

impl Ics<'_> {
    /// Each event as an object of lower-case property names, with `<name>_tzid` for zoned times,
    /// `rdate` and `exdate` as lists and `duration_s` in seconds. Recurrences are not expanded.
    fn events(&self) -> Vec<Value> {
        let mut out = Vec::new();
        let mut event = Map::new();
        let mut in_event = false;
        let mut depth = 0_u32;
        for line in self.lines() {
            let property = Property::parse(&line);
            if !in_event {
                in_event =
                    property.name == "BEGIN" && property.value.eq_ignore_ascii_case("VEVENT");
                continue;
            }
            match property.name.as_str() {
                "BEGIN" => depth = depth.saturating_add(1),
                "END" if depth > 0 => depth = depth.saturating_sub(1),
                "END" => {
                    out.push(Value::Object(mem::take(&mut event)));
                    in_event = false;
                }
                _ if depth > 0 => {}
                "RDATE" | "EXDATE" => {
                    let list = event
                        .entry(property.name.to_ascii_lowercase())
                        .or_insert(json!([]));
                    if let Some(dates) = list.as_array_mut() {
                        dates.extend(property.value.split(',').map(|date| json!(date)));
                    }
                }
                name if KEEP.contains(&name) || TEXT.contains(&name) => {
                    let key = name.to_ascii_lowercase().replace('-', "_");
                    if event.contains_key(&key) {
                        continue;
                    }
                    if let Some(tzid) = property.params.get("TZID") {
                        drop(event.insert(format!("{key}_tzid"), json!(tzid)));
                    }
                    if name == "DURATION" {
                        drop(event.insert("duration_s".to_owned(), json!(property.seconds())));
                    }
                    let text = if TEXT.contains(&name) {
                        property.unescaped()
                    } else {
                        property.value.to_owned()
                    };
                    drop(event.insert(key, json!(text)));
                }
                _ => {}
            }
        }
        out
    }

    /// The document without `DTSTAMP` lines, which some servers set to the time of every download.
    fn fingerprint(&self) -> String {
        self.0
            .lines()
            .filter(|line| !line.starts_with("DTSTAMP"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Unfold the content lines: a line starting with a space or tab continues the previous one.
    fn lines(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for line in self.0.lines() {
            match (line.strip_prefix([' ', '\t']), out.last_mut()) {
                (Some(rest), Some(last)) => last.push_str(rest),
                _ if line.is_empty() => {}
                _ => out.push(line.to_owned()),
            }
        }
        out
    }

    /// Keep only the time and status properties of each event, with its UID hashed, and the time zones.
    fn redact(&self) -> String {
        let mut out = vec!["BEGIN:VCALENDAR".to_owned(), "VERSION:2.0".to_owned()];
        let mut stack: Vec<String> = Vec::new();
        for line in self.lines() {
            let property = Property::parse(&line);
            if property.name == "BEGIN" {
                stack.push(property.value.to_ascii_uppercase());
            }
            // stack[0] is VCALENDAR; alarms and other components nested in an event are dropped.
            match stack.get(1).map(String::as_str) {
                Some("VTIMEZONE") => out.push(line.clone()),
                Some("VEVENT") if stack.len() == 2 => match property.name.as_str() {
                    "UID" => out.push(format!("UID:{:x}", Sha256::digest(property.value))),
                    "BEGIN" | "END" => out.push(line.clone()),
                    name if KEEP.contains(&name) => out.push(line.clone()),
                    _ => {}
                },
                _ => {}
            }
            if property.name == "END" {
                drop(stack.pop());
            }
        }
        out.push("END:VCALENDAR".to_owned());
        let mut text = out.join("\r\n");
        text.push_str("\r\n");
        text
    }

    /// The unfolded lines outside events, and each event's unfolded lines, in the order of `events`, all without
    /// `DTSTAMP`.
    fn split(&self) -> (String, Vec<String>) {
        let mut header = Vec::new();
        let mut blocks = Vec::new();
        let mut block = Vec::new();
        let mut in_event = false;
        let mut depth = 0_u32;
        for line in self.lines() {
            let property = Property::parse(&line);
            let (name, begins_event) = (
                property.name.clone(),
                property.name == "BEGIN" && property.value.eq_ignore_ascii_case("VEVENT"),
            );
            if name == "DTSTAMP" {
                continue;
            }
            if !in_event {
                if begins_event {
                    in_event = true;
                    block.push(line);
                } else {
                    header.push(line);
                }
                continue;
            }
            block.push(line);
            match name.as_str() {
                "BEGIN" => depth = depth.saturating_add(1),
                "END" if depth > 0 => depth = depth.saturating_sub(1),
                "END" => {
                    blocks.push(mem::take(&mut block).join("\n"));
                    in_event = false;
                }
                _ => {}
            }
        }
        (header.join("\n"), blocks)
    }
}

impl<'line> Property<'line> {
    /// Split a content line, minding quoted parameter values.
    fn parse(line: &'line str) -> Self {
        let mut quoted = false;
        let mut name: Option<&str> = None;
        let mut params = BTreeMap::new();
        let mut start = 0_usize;
        for (index, character) in line.char_indices() {
            match character {
                '"' => quoted = !quoted,
                ';' | ':' if !quoted => {
                    let part = line.get(start..index).unwrap_or_default();
                    start = index.saturating_add(1);
                    if name.is_some() {
                        let (key, value) = part.split_once('=').unwrap_or((part, ""));
                        drop(
                            params.insert(
                                key.to_ascii_uppercase(),
                                value.trim_matches('"').to_owned(),
                            ),
                        );
                    } else {
                        name = Some(part);
                    }
                    if character == ':' {
                        return Self {
                            name: name.unwrap_or_default().to_ascii_uppercase(),
                            params,
                            value: line.get(start..).unwrap_or_default(),
                        };
                    }
                }
                _ => {}
            }
        }
        Self {
            name: line.to_ascii_uppercase(),
            params: BTreeMap::new(),
            value: "",
        }
    }

    /// Seconds in a duration value such as `PT1H30M` or `-P1D`. Days count as 24 hours.
    fn seconds(&self) -> Option<i64> {
        let (negative, rest) = self.value.strip_prefix('-').map_or_else(
            || (false, self.value.trim_start_matches('+')),
            |remainder| (true, remainder),
        );
        let Some(body) = rest.strip_prefix('P') else {
            return None;
        };
        let mut total = 0_i64;
        let mut digits = String::new();
        for character in body.chars() {
            let unit: i64 = match character {
                '0'..='9' => {
                    digits.push(character);
                    continue;
                }
                'T' => continue,
                'W' => 604_800,
                'D' => 86_400,
                'H' => 3_600,
                'M' => 60,
                'S' => 1,
                _ => return None,
            };
            let Ok(number) = digits.parse::<i64>() else {
                return None;
            };
            let Some(sum) = number
                .checked_mul(unit)
                .and_then(|part| total.checked_add(part))
            else {
                return None;
            };
            total = sum;
            digits.clear();
        }
        if negative {
            total.checked_neg()
        } else {
            Some(total)
        }
    }

    /// The value with TEXT escaping undone: `\n`, `\,`, `\;` and `\\`.
    fn unescaped(&self) -> String {
        let mut out = String::new();
        let mut chars = self.value.chars();
        while let Some(character) = chars.next() {
            if character != '\\' {
                out.push(character);
                continue;
            }
            match chars.next() {
                Some('n' | 'N') => out.push('\n'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        }
        out
    }
}

#[async_trait]
impl Source for Calendar {
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(config) = app.cfg.calendars.as_deref() else {
            info!("calendar: not configured, skipping");
            return Ok(());
        };
        // serde errors can quote the value, which holds the URLs, so report only the position.
        let feeds: BTreeMap<String, Feed> = tri!(serde_json::from_str(config).map_err(|error| {
            anyhow!(
                "CALENDARS is not a JSON object of {{\"url\", \"redact\"}} (line {}, column {})",
                error.line(),
                error.column()
            )
        }));
        let now = Utc::now();
        // Store whatever arrived, so one failing calendar does not hold back the others.
        let mut files = Vec::new();
        let mut states = Vec::new();
        let mut failure = None;
        for (name, feed) in &feeds {
            match feed.snapshot(app, name, now).await {
                Ok((file, state)) => {
                    files.extend(file);
                    states.extend(state);
                }
                Err(error) => failure = failure.or(Some(error)),
            }
        }
        if !files.is_empty() {
            let kinds: Vec<&str> = files
                .iter()
                .filter_map(|file| file.path.split('/').nth(2))
                .collect();
            let message = format!("calendar: {}", kinds.join(", "));
            drop(tri!(app.save(&files, &message).await));
        }
        for (key, value) in states {
            tri!(app.store.set(&key, &value));
        }
        failure.map_or(Ok(()), Err)
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// The identity of an event within its calendar: `uid|recurrence-id`, the second part empty for the series.
fn event_key(event: &Value) -> String {
    let part = |name: &str| event.get(name).and_then(Value::as_str).unwrap_or_default();
    format!("{}|{}", part("uid"), part("recurrence_id"))
}
