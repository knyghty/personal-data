//! Read this Mac's app usage from knowledgeC, app focus and media records from Biome, and the Biome device names.

#[cfg(test)]
pub mod tests;

use alloc::collections::{BTreeMap, BTreeSet};
use core::ops::{Add as _, Div as _, Mul as _, Sub as _};
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::fs::{self, DirEntry};
use std::path::Path;
use std::{env, io, process};

use anyhow::{Context as _, Result};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use rusqlite::{Connection, OpenFlags};
use serde::ser::SerializeStruct as _;
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::segb::{self, Field, Record, Segment};
use crate::tri;

/// Seconds from the Unix epoch to 2001-01-01, the epoch of Core Data and Biome times.
const APPLE_EPOCH: f64 = 978_307_200.0;
/// Records per stream and device in one push, so a first push of weeks of history stays small; the rest follows.
const MAX_RAW_RECORDS: usize = 5_000;
/// Read knowledgeC again this far before its cursor, for rows written late.
const OVERLAP: f64 = 3600.0;
/// Streams sent raw besides media, to map once their records are seen: context and behaviour, not content.
/// Streams of messages, mail, notes, calls and the pasteboard are left out on purpose.
const RAW_STREAMS: &[&str] = &[
    "restricted/App.WebUsage",
    "restricted/Audio.Route",
    "restricted/CarPlay.Connected",
    "restricted/Clock.Alarm",
    "restricted/Device.Power.LowPowerMode",
    "restricted/Device.Wireless.AirplaneMode",
    "restricted/Device.Wireless.Bluetooth",
    "restricted/Device.Wireless.WiFi",
    "restricted/GameController.Connected",
    "restricted/HomeKit.Client.AccessoryControl",
    "restricted/HomeKit.Client.ActionSet",
    "restricted/Location.MicroLocationVisit",
    "restricted/Location.PointOfInterest.Category",
    "restricted/Location.Semantic",
    "restricted/Motion.Activity",
    "restricted/NewsArticleView",
    "restricted/Notification.Usage",
    "restricted/PhotosPhotoView",
    "restricted/SafariPageView",
    "restricted/Screen.Sharing",
    "restricted/ScreenTime.AppUsage",
    "restricted/UserFocus.ComputedMode",
    "restricted/_DKEvent.Keybag.IsLocked",
    "restricted/_DKEvent.Wifi.Connection",
];

/// The Biome directory, `Library/Biome`.
pub(super) struct Biome<'dir> {
    /// The directory.
    pub root: &'dir Path,
}

impl Biome<'_> {
    /// Names of the Biome streams, as `public/<name>` and `restricted/<name>`.
    pub(crate) fn streams(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for kind in ["public", "restricted"] {
            for entry in tri!(list(&self.root.join("streams").join(kind))) {
                if tri!(entry.file_type()).is_dir() {
                    out.push(format!("{kind}/{}", entry.file_name().to_string_lossy()));
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

/// A read-only connection to a copy of a live database, unlinked once open.
pub(super) struct Copy {
    /// The connection.
    pub conn: Connection,
}

/// A device Biome syncs with.
#[derive(Serialize, Clone)]
pub(super) struct Device {
    /// `DevicePeer.name`.
    pub name: Option<String>,
    /// `ios`, `macos`, `tvos` or `unknown`.
    pub platform: String,
    /// `DevicePeer.device_identifier`, or `local` for this Mac.
    #[serde(rename = "device_uuid")]
    pub uuid: String,
}

/// One `App.InFocus` transition; `Serialize` keeps the fields in the order the collector has always received them.
pub(super) struct Focus {
    /// The app's bundle id.
    pub bundle: String,
    /// The device's name, when `sync.db` knows it.
    pub device_name: Option<String>,
    /// The device directory: `local` or the remote uuid.
    pub device_uuid: String,
    /// Whether the app gained focus; it lost focus otherwise.
    pub gained: bool,
    /// The device's platform, or `unknown`.
    pub platform: String,
    /// The transition time as RFC 3339 UTC.
    pub time: String,
}

impl Serialize for Focus {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("Focus", 6));
        tri!(state.serialize_field("device_uuid", &self.device_uuid));
        tri!(state.serialize_field("device_name", &self.device_name));
        tri!(state.serialize_field("platform", &self.platform));
        tri!(state.serialize_field("bundle", &self.bundle));
        tri!(state.serialize_field("time", &self.time));
        tri!(state.serialize_field("gained", &self.gained));
        state.end()
    }
}

/// The knowledgeC database, `Library/Application Support/Knowledge/knowledgeC.db`.
pub(super) struct Knowledge<'db> {
    /// The database.
    pub db: &'db Path,
}

impl Knowledge<'_> {
    /// `/app/usage` rows of this Mac (no source device) created after `since` less the overlap, without the ones in
    /// `seen`, and the newest creation time. Rows are written when an interval ends, so the cursor is the creation
    /// time.
    pub(crate) fn usage(&self, since: f64, seen: &BTreeSet<String>) -> Result<Usage> {
        let copy = tri!(open_copy(self.db));
        let mut stmt = tri!(copy.conn.prepare(
            "SELECT o.ZVALUESTRING, o.ZSTARTDATE, o.ZENDDATE, o.ZCREATIONDATE
             FROM ZOBJECT o LEFT JOIN ZSOURCE s ON s.Z_PK = o.ZSOURCE
             WHERE o.ZSTREAMNAME = '/app/usage' AND s.ZDEVICEID IS NULL AND o.ZCREATIONDATE > ?1
             ORDER BY o.ZCREATIONDATE",
        ));
        let mut usage = Usage {
            cursor: since,
            rows: Vec::new(),
            seen: BTreeSet::new(),
        };
        let rows = tri!(stmt.query_map([since.sub(OVERLAP)], |row| {
            Ok((
                tri!(row.get::<_, Option<String>>(0)),
                tri!(row.get::<_, f64>(1)),
                tri!(row.get::<_, f64>(2)),
                tri!(row.get::<_, f64>(3)),
            ))
        }));
        for row in rows {
            let (bundle, start, end, created) = tri!(row);
            usage.cursor = usage.cursor.max(created);
            // Zero-length rows exist and carry no usage.
            if let Some(name) = bundle.filter(|text| !text.is_empty() && end > start) {
                let start_iso = iso(start);
                usage.rows.push(MacUsage {
                    key: format!("{start_iso}|{name}"),
                    bundle: name,
                    created: iso(created),
                    created_s: created,
                    end: iso(end),
                    start: start_iso,
                });
            }
        }
        // The next read returns the rows within the overlap again.
        let floor = usage.cursor.sub(OVERLAP);
        usage.seen = usage
            .rows
            .iter()
            .filter(|row| row.created_s > floor)
            .map(|row| row.key.clone())
            .collect();
        usage.rows.retain(|row| !seen.contains(&row.key));
        Ok(usage)
    }
}

/// One knowledgeC `/app/usage` row; `Serialize` keeps the fields in the order the collector has always received
/// them.
pub(super) struct MacUsage {
    /// The app's bundle id.
    pub bundle: String,
    /// `ZCREATIONDATE` as RFC 3339 UTC.
    pub created: String,
    /// `created` in seconds since 2001.
    pub created_s: f64,
    /// `ZENDDATE` as RFC 3339 UTC.
    pub end: String,
    /// `<start>|<bundle>`, which identifies the row across runs.
    pub key: String,
    /// `ZSTARTDATE` as RFC 3339 UTC.
    pub start: String,
}

impl Serialize for MacUsage {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("MacUsage", 4));
        tri!(state.serialize_field("bundle", &self.bundle));
        tri!(state.serialize_field("start", &self.start));
        tri!(state.serialize_field("end", &self.end));
        tri!(state.serialize_field("created", &self.created));
        state.end()
    }
}

/// One raw record of a media or context stream; `Serialize` keeps the fields in the order the collector has always
/// received them.
pub(super) struct Media {
    /// The device directory: `local` or the remote uuid.
    pub device_uuid: String,
    /// The record's protobuf fields, field number to value.
    pub fields: Value,
    /// The stream name, `<kind>/<name>`.
    pub stream: String,
    /// The record time as RFC 3339 UTC.
    pub time: String,
}

impl Serialize for Media {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("Media", 4));
        tri!(state.serialize_field("stream", &self.stream));
        tri!(state.serialize_field("device_uuid", &self.device_uuid));
        tri!(state.serialize_field("time", &self.time));
        tri!(state.serialize_field("fields", &self.fields));
        state.end()
    }
}

/// A Biome stream, at `<biome>/streams/<name>`, with a `local` (this Mac) and `remote/<uuid>` directory per device.
pub(super) struct Stream<'stream> {
    /// The Biome directory.
    pub biome: &'stream Path,
    /// `<kind>/<name>`, as `Biome::streams` lists it.
    pub name: &'stream str,
}

impl Stream<'_> {
    /// App focus transitions from `App.InFocus`: field 3 is 1 when focus is gained and 0 when lost, field 4 the
    /// time (f64, seconds since 2001) and field 6 the bundle id. Returns transitions after each device's cursor and
    /// moves it to the newest.
    pub(crate) fn focus(
        &self,
        devices: &[Device],
        cursors: &mut BTreeMap<String, f64>,
    ) -> Result<Vec<Focus>> {
        let key = |device: &str| format!("App.InFocus/{device}");
        let mut out = Vec::new();
        for (device, records) in
            tri!(self.segments(|device| { cursors.get(&key(device)).copied().unwrap_or(0.0_f64) }))
        {
            let known = devices.iter().find(|peer| peer.uuid == device);
            let since = cursors.get(&key(&device)).copied().unwrap_or(0.0_f64);
            let mut newest = since;
            for record in records {
                let Some(fields) = segb::decode(&record.data) else {
                    continue;
                };
                let (mut gained, mut time, mut bundle) = (false, None, None);
                for (number, field) in fields {
                    match (number, field) {
                        (3, Field::Varint(value)) => gained = value == 1,
                        (4, Field::Fixed64(value)) => time = Some(value),
                        (6, Field::Bytes(bytes)) => bundle = str::from_utf8(bytes).ok(),
                        _ => {}
                    }
                }
                let (Some(at), Some(name)) = (time, bundle.filter(|text| !text.is_empty())) else {
                    continue;
                };
                if at <= since {
                    continue;
                }
                newest = newest.max(at);
                out.push(Focus {
                    bundle: name.to_owned(),
                    device_name: known.and_then(|peer| peer.name.clone()),
                    device_uuid: device.clone(),
                    gained,
                    platform: known
                        .map_or_else(|| "unknown".to_owned(), |peer| peer.platform.clone()),
                    time: iso(at),
                });
            }
            let _: Option<f64> = cursors.insert(key(&device), newest);
        }
        Ok(out)
    }

    /// Whether the name suggests now-playing media.
    #[must_use]
    pub(crate) fn is_media(&self) -> bool {
        let lower = self.name.to_ascii_lowercase();
        lower.contains("nowplaying") || lower.contains("media")
    }

    /// Whether the stream's records are sent raw: media and the streams in `RAW_STREAMS`.
    #[must_use]
    pub(crate) fn is_raw(&self) -> bool {
        self.is_media() || RAW_STREAMS.contains(&self.name)
    }

    /// Records after each device's cursor, as raw protobuf fields (field number to value), timed by the SEGB entry.
    pub(crate) fn media(&self, cursors: &mut BTreeMap<String, f64>) -> Result<Vec<Media>> {
        let key = |device: &str| format!("{}/{device}", self.name);
        let mut out = Vec::new();
        for (device, records) in
            tri!(self.segments(|device| { cursors.get(&key(device)).copied().unwrap_or(0.0_f64) }))
        {
            let since = cursors.get(&key(&device)).copied().unwrap_or(0.0_f64);
            let mut fresh: Vec<Record> = records
                .into_iter()
                .filter(|record| record.time > since)
                .collect();
            // ponytail: a cut inside records sharing one time skips the rest of them; fine at millisecond times.
            fresh.sort_by(|left, right| left.time.total_cmp(&right.time));
            fresh.truncate(MAX_RAW_RECORDS);
            let mut newest = since;
            for record in fresh {
                newest = newest.max(record.time);
                let fields =
                    segb::to_json(&record.data, 0).unwrap_or_else(|| segb::hex(&record.data));
                out.push(Media {
                    device_uuid: device.clone(),
                    fields,
                    stream: self.name.to_owned(),
                    time: iso(record.time),
                });
            }
            let _: Option<f64> = cursors.insert(key(&device), newest);
        }
        Ok(out)
    }

    /// The written records of each device directory: `local` (this Mac) and `remote/<uuid>`.
    /// A segment file is named by its creation time (microseconds since 2001) and Biome appends only to the newest,
    /// so a file is skipped when the next one was created an hour or more before `since(device)`. Modification times
    /// are no use: Biome writes to its preallocated files without changing them. Unreadable segments are logged.
    fn segments<Since>(&self, since: Since) -> Result<Vec<(String, Vec<Record>)>>
    where
        Since: Fn(&str) -> f64,
    {
        let stream = self.biome.join("streams").join(self.name);
        let mut dirs = vec![("local".to_owned(), stream.join("local"))];
        for entry in tri!(list(&stream.join("remote"))) {
            if tri!(entry.file_type()).is_dir() {
                dirs.push((
                    entry.file_name().to_string_lossy().into_owned(),
                    entry.path(),
                ));
            }
        }
        let mut out = Vec::new();
        for (device, dir) in dirs {
            let after = since(&device).sub(OVERLAP);
            let mut files = Vec::new();
            for entry in tri!(list(&dir)) {
                if !tri!(entry.file_type()).is_dir() {
                    files.push(entry);
                }
            }
            files.sort_by(|left, right| {
                created(left)
                    .unwrap_or(f64::MAX)
                    .total_cmp(&created(right).unwrap_or(f64::MAX))
            });
            let mut records = Vec::new();
            for (index, entry) in files.iter().enumerate() {
                if index
                    .checked_add(1)
                    .and_then(|next| files.get(next))
                    .and_then(created)
                    .is_some_and(|next| next < after)
                {
                    continue;
                }
                let bytes = tri!(
                    fs::read(entry.path())
                        .with_context(|| format!("read {}", entry.path().display()))
                );
                match (Segment { bytes: &bytes }).written() {
                    Ok(found) => records.extend(found),
                    Err(error) => crate::log(&format!("skip {}: {error}", entry.path().display())),
                }
            }
            out.push((device, records));
        }
        Ok(out)
    }
}

/// The Biome sync database, `Library/Biome/sync/sync.db`, and this Mac's name.
pub(super) struct SyncDb<'db> {
    /// The database.
    pub db: &'db Path,
    /// This Mac's name, for the `local` device.
    pub host: &'db str,
}

impl SyncDb<'_> {
    /// The devices Biome syncs with, from `DevicePeer`, plus this Mac as `local`.
    pub(crate) fn devices(&self) -> Result<Vec<Device>> {
        let mut out = vec![Device {
            name: Some(self.host.to_owned()),
            platform: "macos".to_owned(),
            uuid: "local".to_owned(),
        }];
        if !self.db.exists() {
            return Ok(out);
        }
        let copy = tri!(open_copy(self.db));
        let mut stmt = tri!(
            copy.conn
                .prepare("SELECT device_identifier, name, platform FROM DevicePeer")
        );
        let rows = tri!(stmt.query_map([], |row| {
            // The `DevicePeer.platform` codes: 2 iOS and iPadOS, 4 macOS, 5 tvOS.
            let platform = match tri!(row.get::<_, Option<i64>>(2)) {
                Some(2) => "ios".to_owned(),
                Some(4) => "macos".to_owned(),
                Some(5) => "tvos".to_owned(),
                Some(code) => format!("unknown-{code}"),
                None => "unknown".to_owned(),
            };
            Ok(Device {
                name: tri!(row.get(1)),
                platform,
                uuid: tri!(row.get(0)),
            })
        }));
        for device in rows {
            out.push(tri!(device));
        }
        Ok(out)
    }
}

/// One read of knowledgeC.
pub(super) struct Usage {
    /// The newest creation time, in seconds since 2001: the next cursor.
    pub cursor: f64,
    /// The rows not sent before.
    pub rows: Vec<MacUsage>,
    /// The keys of the rows within the overlap of `cursor`, to skip when the next read returns them again.
    pub seen: BTreeSet<String>,
}

/// Seconds since 2001 that a segment file was created, from its name; `None` for a name that is not a number.
fn created(entry: &DirEntry) -> Option<f64> {
    let name = entry.file_name();
    let Some(micros) = name.to_str().and_then(|text| text.parse::<u64>().ok()) else {
        return None;
    };
    let whole = micros
        .checked_div(1_000_000)
        .and_then(|seconds| u32::try_from(seconds).ok())
        .map(f64::from);
    let part = micros
        .checked_rem(1_000_000)
        .and_then(|rest| u32::try_from(rest).ok())
        .map(f64::from);
    whole
        .zip(part)
        .map(|(seconds, rest)| rest.div(1e6_f64).add(seconds))
}

/// A time in seconds since 2001 as RFC 3339 UTC, rounded to the millisecond; empty when out of range.
pub(super) fn iso(seconds: f64) -> String {
    let millis = seconds.add(APPLE_EPOCH).mul(1_000.0_f64).round();
    // Whole seconds and the millisecond part convert to `Duration` exactly; the sum of the two would not.
    let whole = millis.div_euclid(1_000.0_f64);
    let (Ok(whole_part), Ok(millis_part)) = (
        Duration::try_from_secs_f64(whole.abs()),
        Duration::try_from_secs_f64(millis.rem_euclid(1_000.0_f64).div(1_000.0_f64)),
    ) else {
        return String::new();
    };
    let (Ok(whole_delta), Ok(millis_delta)) = (
        TimeDelta::from_std(whole_part),
        TimeDelta::from_std(millis_part),
    ) else {
        return String::new();
    };
    let unix = DateTime::<Utc>::UNIX_EPOCH;
    let start = if whole < 0.0_f64 {
        unix.checked_sub_signed(whole_delta)
    } else {
        unix.checked_add_signed(whole_delta)
    };
    start
        .and_then(|at| at.checked_add_signed(millis_delta))
        .map_or_default(|at| at.to_rfc3339_opts(SecondsFormat::Millis, true))
}

/// Directory entries, or none when the directory does not exist.
pub(super) fn list(dir: &Path) -> Result<Vec<DirEntry>> {
    match fs::read_dir(dir) {
        Ok(entries) => Ok(tri!(entries.collect::<io::Result<_>>())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("list {}", dir.display())),
    }
}

/// Copy a live WAL database with its `-wal` and `-shm` to a temporary directory and open the copy read-only, so the
/// original is never locked or checkpointed. The first read opens the side files too; the directory is then
/// unlinked, so the connection holds the last references and nothing is left behind when it closes.
pub(super) fn open_copy(db: &Path) -> Result<Copy> {
    static COUNT: AtomicU32 = AtomicU32::new(0);
    let dir = env::temp_dir().join(format!(
        "personal-data-mac-push-{}-{}",
        process::id(),
        COUNT.fetch_add(1, Ordering::Relaxed)
    ));
    tri!(fs::create_dir_all(&dir));
    let open = || -> Result<Connection> {
        let name = tri!(db.file_name().context("database path has no file name"));
        let copy = dir.join(name);
        let _: u64 = tri!(fs::copy(db, &copy).with_context(|| format!("copy {}", db.display())));
        for ext in ["-wal", "-shm"] {
            let mut side = db.as_os_str().to_owned();
            side.push(ext);
            let mut to = copy.as_os_str().to_owned();
            to.push(ext);
            match fs::copy(&side, &to) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                result => {
                    let _: u64 =
                        tri!(result.with_context(|| format!("copy {}{ext}", db.display())));
                }
            }
        }
        let conn = tri!(Connection::open_with_flags(
            &copy,
            OpenFlags::SQLITE_OPEN_READ_ONLY
        ));
        let _: i64 =
            tri!(conn.query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0)));
        Ok(conn)
    };
    let opened = open();
    let removed = fs::remove_dir_all(&dir).with_context(|| format!("remove {}", dir.display()));
    let conn = tri!(opened);
    tri!(removed);
    Ok(Copy { conn })
}
