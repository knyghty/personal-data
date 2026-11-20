//! Read Safari and Firefox history visits, with full URLs, from copies of their live databases.
//!
//! Both browsers also store visits synced from other devices (iCloud, Firefox Sync) and insert them when they
//! arrive, with their original time. So the cursor is the visit id, which grows with every insert, not the visit
//! time, which would skip visits synced late.

#[cfg(test)]
pub mod tests;

use std::fs;
use std::io;
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, SecondsFormat};
use rusqlite::{Connection, params};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::apple::{iso, list, open_copy};
use crate::tri;

/// The Firefox profiles directory.
pub(super) struct Firefox<'dir> {
    /// `Library/Application Support/Firefox/Profiles`.
    pub profiles: &'dir Path,
}

/// The Orion history database of the default profile.
pub(super) struct Orion<'db> {
    /// `Library/Application Support/Orion/Defaults/history`.
    pub db: &'db Path,
}

/// The Safari history database.
pub(super) struct Safari<'db> {
    /// `Library/Safari/History.db`.
    pub db: &'db Path,
}

/// One history visit; `Serialize` keeps the fields in the order the collector has always received them.
#[derive(Debug)]
pub(super) struct HistoryVisit {
    /// `safari`, `firefox` or `orion`.
    pub browser: &'static str,
    /// Columns of `history_visits` whose name contains `device`, by name, when Safari has any.
    pub device: Option<Value>,
    /// Firefox `moz_historyvisits.from_visit` or Orion `visits.REFERRING_VISIT_ID`.
    pub from_visit: Option<i64>,
    /// Safari `history_visits.load_successful`.
    pub load_successful: Option<bool>,
    /// Safari `history_visits.origin` or Firefox `moz_historyvisits.source`, raw.
    pub origin_or_source: Option<i64>,
    /// The Firefox profile directory name, or `default` for Safari.
    pub profile: String,
    /// Safari `history_visits.redirect_destination`.
    pub redirect_destination: Option<i64>,
    /// Safari `history_visits.redirect_source`.
    pub redirect_source: Option<i64>,
    /// The visit time as RFC 3339 UTC.
    pub time: String,
    /// The page title, when the browser stored one.
    pub title: Option<String>,
    /// Orion `visits.TRANSITION_TYPE`, e.g. `link`, `typed`, `start_page`.
    pub transition: Option<String>,
    /// The full URL.
    pub url: String,
    /// The visit's row id, which is the cursor.
    pub visit_id: i64,
    /// Firefox `moz_historyvisits.visit_type`.
    pub visit_type: Option<i64>,
}

impl Firefox<'_> {
    /// Each profile directory under `profiles` that has a `places.sqlite`, by directory name.
    ///
    /// # Errors
    ///
    /// Fails when the directory or a profile cannot be read.
    pub(crate) fn profiles(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in tri!(list(self.profiles)) {
            let db = entry.path().join("places.sqlite");
            if tri!(entry.file_type()).is_dir() && tri!(exists(&db)) {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    /// Up to `limit` visits of `profile` with an id after `since`, oldest id first.
    ///
    /// `visit_date` is in microseconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Fails when the profile's `places.sqlite` cannot be copied or read.
    // ponytail: ids are rowids without AUTOINCREMENT, so after the newest visits are deleted (Forget, Clear Recent
    // History) Firefox reuses their ids and the next visits are skipped; cursor on (id, visit_date) if that matters.
    pub(crate) fn visits(
        &self,
        profile: &str,
        since: i64,
        limit: usize,
    ) -> Result<Vec<HistoryVisit>> {
        let copy = tri!(open_copy(
            &self.profiles.join(profile).join("places.sqlite")
        ));
        let have = tri!(columns(&copy.conn, "moz_historyvisits"));
        let sql = format!(
            "SELECT v.id, v.visit_date, p.url, p.title, {}, v.visit_type, v.from_visit
             FROM moz_historyvisits v JOIN moz_places p ON p.id = v.place_id
             WHERE v.id > ?1 AND v.visit_date IS NOT NULL ORDER BY v.id LIMIT ?2",
            col(&have, "v", "source"),
        );
        let cap = tri!(i64::try_from(limit));
        let mut stmt = tri!(copy.conn.prepare(&sql));
        let rows = tri!(stmt.query_map(params![since, cap], |row| {
            Ok(HistoryVisit {
                browser: "firefox",
                device: None,
                from_visit: tri!(row.get(6)),
                load_successful: None,
                origin_or_source: tri!(row.get(4)),
                profile: profile.to_owned(),
                redirect_destination: None,
                redirect_source: None,
                time: DateTime::from_timestamp_micros(tri!(row.get(1)))
                    .map_or_default(|date| date.to_rfc3339_opts(SecondsFormat::Millis, true)),
                title: tri!(row.get(3)),
                transition: None,
                url: tri!(row.get(2)),
                visit_id: tri!(row.get(0)),
                visit_type: tri!(row.get(5)),
            })
        }));
        let visits = tri!(rows.collect::<rusqlite::Result<Vec<HistoryVisit>>>());
        Ok(visits)
    }
}

impl Orion<'_> {
    /// Up to `limit` visits with an id after `since`, oldest id first.
    ///
    /// `VISIT_TIME` is UTC text; the title is the page's latest, as Orion keeps none per visit.
    ///
    /// # Errors
    ///
    /// Fails when the database cannot be copied or read.
    pub(crate) fn visits(&self, since: i64, limit: usize) -> Result<Vec<HistoryVisit>> {
        if !tri!(exists(self.db)) {
            return Ok(Vec::new());
        }
        let copy = tri!(open_copy(self.db));
        let cap = tri!(i64::try_from(limit));
        let mut stmt = tri!(copy.conn.prepare(
            "SELECT v.ID, strftime('%Y-%m-%dT%H:%M:%fZ', v.VISIT_TIME), i.URL, i.TITLE,
                    nullif(v.REFERRING_VISIT_ID, 0), v.TRANSITION_TYPE
             FROM visits v JOIN history_items i ON i.ID = v.HISTORY_ITEM_ID
             WHERE v.ID > ?1 ORDER BY v.ID LIMIT ?2"
        ));
        let rows = tri!(stmt.query_map(params![since, cap], |row| {
            Ok(HistoryVisit {
                browser: "orion",
                device: None,
                from_visit: tri!(row.get(4)),
                load_successful: None,
                origin_or_source: None,
                profile: "default".to_owned(),
                redirect_destination: None,
                redirect_source: None,
                time: tri!(row.get(1)),
                title: tri!(row.get(3)),
                transition: tri!(row.get(5)),
                url: tri!(row.get(2)),
                visit_id: tri!(row.get(0)),
                visit_type: None,
            })
        }));
        let visits = tri!(rows.collect::<rusqlite::Result<Vec<HistoryVisit>>>());
        Ok(visits)
    }
}

impl Safari<'_> {
    /// Up to `limit` visits with an id after `since`, oldest id first.
    ///
    /// `visit_time` is in seconds since 2001.
    ///
    /// # Errors
    ///
    /// Fails when the database cannot be copied or read.
    pub(crate) fn visits(&self, since: i64, limit: usize) -> Result<Vec<HistoryVisit>> {
        if !tri!(exists(self.db)) {
            return Ok(Vec::new());
        }
        let copy = tri!(open_copy(self.db));
        let have = tri!(columns(&copy.conn, "history_visits"));
        let devices: Vec<&String> = have.iter().filter(|name| name.contains("device")).collect();
        let device = if devices.is_empty() {
            "NULL".to_owned()
        } else {
            let pairs: Vec<String> = devices
                .iter()
                .map(|name| format!("'{name}', v.\"{name}\""))
                .collect();
            format!("json_object({})", pairs.join(", "))
        };
        let sql = format!(
            "SELECT v.id, v.visit_time, i.url, v.title, {}, {}, {}, {}, {device}
             FROM history_visits v JOIN history_items i ON i.id = v.history_item
             WHERE v.id > ?1 ORDER BY v.id LIMIT ?2",
            col(&have, "v", "origin"),
            col(&have, "v", "load_successful"),
            col(&have, "v", "redirect_source"),
            col(&have, "v", "redirect_destination"),
        );
        let cap = tri!(i64::try_from(limit));
        let mut stmt = tri!(copy.conn.prepare(&sql));
        let rows = tri!(stmt.query_map(params![since, cap], |row| {
            Ok(HistoryVisit {
                browser: "safari",
                device: tri!(row.get::<_, Option<String>>(8))
                    .and_then(|json| serde_json::from_str(&json).ok()),
                from_visit: None,
                load_successful: tri!(row.get(5)),
                origin_or_source: tri!(row.get(4)),
                profile: "default".to_owned(),
                redirect_destination: tri!(row.get(7)),
                redirect_source: tri!(row.get(6)),
                time: iso(tri!(row.get(1))),
                title: tri!(row.get(3)),
                transition: None,
                url: tri!(row.get(2)),
                visit_id: tri!(row.get(0)),
                visit_type: None,
            })
        }));
        let visits = tri!(rows.collect::<rusqlite::Result<Vec<HistoryVisit>>>());
        Ok(visits)
    }
}

impl Serialize for HistoryVisit {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("Visit", 14));
        tri!(state.serialize_field("browser", self.browser));
        tri!(state.serialize_field("profile", &self.profile));
        tri!(state.serialize_field("visit_id", &self.visit_id));
        tri!(state.serialize_field("time", &self.time));
        tri!(state.serialize_field("url", &self.url));
        tri!(state.serialize_field("title", &self.title));
        tri!(state.serialize_field("origin_or_source", &self.origin_or_source));
        tri!(optional(&mut state, "visit_type", self.visit_type.as_ref()));
        tri!(optional(&mut state, "from_visit", self.from_visit.as_ref()));
        tri!(optional(
            &mut state,
            "load_successful",
            self.load_successful.as_ref()
        ));
        tri!(optional(
            &mut state,
            "redirect_source",
            self.redirect_source.as_ref()
        ));
        tri!(optional(
            &mut state,
            "redirect_destination",
            self.redirect_destination.as_ref()
        ));
        tri!(optional(&mut state, "device", self.device.as_ref()));
        tri!(optional(&mut state, "transition", self.transition.as_ref()));
        state.end()
    }
}

/// `alias.name` when the table has the column, else NULL, so an app version without it still reads.
pub(super) fn col(have: &[String], alias: &str, name: &str) -> String {
    if have.iter().any(|column| column == name) {
        format!("{alias}.\"{name}\"")
    } else {
        "NULL".to_owned()
    }
}

/// The columns of a table.
///
/// # Errors
///
/// Fails when the query fails.
pub(super) fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = tri!(conn.prepare("SELECT name FROM pragma_table_info(?1)"));
    let names = tri!(stmt.query_map([table], |row| row.get(0)));
    let found = tri!(names.collect::<rusqlite::Result<Vec<String>>>());
    Ok(found)
}

/// Whether a database exists.
///
/// Missing access is an error, so Full Disk Access is still asked for.
///
/// # Errors
///
/// Fails when the metadata cannot be read for a reason other than a missing file.
pub(super) fn exists(db: &Path) -> Result<bool> {
    match fs::metadata(db) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Write `key` only when `value` is `Some`, as `skip_serializing_if = "Option::is_none"` does.
///
/// # Errors
///
/// Fails when the serializer fails.
pub(super) fn optional<State, Field>(
    state: &mut State,
    key: &'static str,
    value: Option<&Field>,
) -> Result<(), State::Error>
where
    State: SerializeStruct,
    Field: Serialize,
{
    match value {
        Some(field) => state.serialize_field(key, field),
        None => state.skip_field(key),
    }
}
