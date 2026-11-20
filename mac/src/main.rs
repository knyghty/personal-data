//! Push this Mac's app usage, the app focus of the iPhones and iPads that share Screen Time with it, Biome media
//! and context records, Claude Code activity, Safari, Firefox and Orion history, Photos metadata, Podcasts play state,
//! Obsidian note metadata, shell command history and the aircraft polled near home to the collector. launchd runs it
//! every 15 minutes, and `--flights` every minute.

extern crate alloc;

pub mod apple;
pub mod browser;
pub mod claude;
pub mod flights;
pub mod obsidian;
pub mod photos;
pub mod podcasts;
pub mod segb;
pub mod shell;
#[cfg(test)]
mod tests;
mod tri;

use alloc::collections::{BTreeMap, BTreeSet};
use core::time::Duration;
use std::env;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use rusqlite::types::ValueRef;
use serde::ser::SerializeStruct as _;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use ureq::Agent;

use crate::flights::{Area, Flights};
use crate::shell::{History, Shell};

/// Browser visits to send per run; the rest waits for the next run.
const BROWSER_CAP: usize = 20_000;
/// Bytes of Claude Code session logs to read per run; the rest waits for the next run.
const CLAUDE_BUDGET: u64 = 64 << 20;
/// The collector's ingest endpoint, written into a new config.
const DEFAULT_URL: &str = "https://glimmering-voice-610.fly.dev/screen-time/ingest";
/// Obsidian notes to send per run; the rest waits for the next run.
const OBSIDIAN_CAP: usize = 5_000;
/// The Obsidian vault iCloud syncs, under home.
const OBSIDIAN_VAULT: &str = "Library/Mobile Documents/iCloud~md~obsidian/Documents";
/// Photos assets to send per run; the rest waits for the next run.
const PHOTOS_CAP: usize = 20_000;
/// Podcast episodes to send per run; the rest waits for the next run.
const PODCASTS_CAP: usize = 5_000;
/// The Podcasts library, under home.
const PODCASTS_DB: &str =
    "Library/Group Containers/243LU875E5.groups.com.apple.podcasts/Documents/MTLibrary.sqlite";
/// Shell commands to send per run and shell; the rest waits for the next run.
const SHELL_CAP: usize = 20_000;

/// What `config.json` holds.
#[derive(Serialize, Deserialize)]
struct Config {
    /// Where `--flights` looks for aircraft; without it, it does nothing.
    #[serde(default)]
    flights: Option<Area>,
    /// The bearer token the collector expects.
    secret: String,
    /// The collector's ingest endpoint.
    url: String,
}

impl Config {
    /// One `--flights` poll into `support`, when an area is set.
    fn poll(&self, support: &Path) -> Result<()> {
        self.flights
            .as_ref()
            .map_or(Ok(()), |area| Flights { support }.poll(area))
    }

    /// POST `body` to the collector.
    fn push(&self, body: &str) -> Result<()> {
        let agent: Agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_mins(5)))
            .build()
            .into();
        drop(tri!(
            agent
                .post(&self.url)
                .header("Authorization", &format!("Bearer {}", self.secret))
                .header("Content-Type", "application/json")
                .send(body)
                .with_context(|| format!("POST {}", self.url))
        ));
        Ok(())
    }
}

/// The Mac the helper runs on.
#[derive(Clone, Copy, Debug)]
struct Mac<'home> {
    /// The user's home directory, under which everything is read.
    home: &'home Path,
}

impl Mac<'_> {
    /// Read everything new since `state`, and the state to keep once it is sent.
    fn collect(self, host: &str, state: &State) -> Result<(Payload, State)> {
        let mut next = state.clone();
        let mut payload = Payload {
            host: host.to_owned(),
            ..Payload::default()
        };
        tri!(self.collect_apple(host, state, &mut next, &mut payload));
        tri!(self.collect_claude(&mut next, &mut payload));
        tri!(self.collect_obsidian(state, &mut next, &mut payload));
        tri!(self.collect_browsers(state, &mut next, &mut payload));
        tri!(self.collect_photos(state, &mut next, &mut payload));
        tri!(self.collect_podcasts(state, &mut next, &mut payload));
        tri!(self.collect_shells(state, &mut next, &mut payload));
        payload.sent_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok((payload, next))
    }

    /// Read knowledgeC, the Biome sync peers and the Biome streams.
    fn collect_apple(
        self,
        host: &str,
        state: &State,
        next: &mut State,
        payload: &mut Payload,
    ) -> Result<()> {
        let biome = self.home.join("Library/Biome");

        if let Some(usage) = tri!(soft(
            "knowledgeC",
            apple::Knowledge {
                db: &self
                    .home
                    .join("Library/Application Support/Knowledge/knowledgeC.db"),
            }
            .usage(state.mac_created, &state.mac_seen),
        )) {
            next.mac_created = usage.cursor;
            next.mac_seen = usage.seen;
            payload.mac_usage = usage.rows;
        }

        let devices = tri!(soft(
            "sync.db",
            apple::SyncDb {
                db: &biome.join("sync/sync.db"),
                host,
            }
            .devices(),
        ))
        .unwrap_or_default();

        let mut cursors = next.biome.clone();
        payload.focus = tri!(soft(
            "App.InFocus",
            apple::Stream {
                biome: &biome,
                name: "restricted/App.InFocus",
            }
            .focus(&devices, &mut cursors),
        ))
        .unwrap_or_default();

        let streams = tri!(soft(
            "Biome streams",
            apple::Biome { root: &biome }.streams()
        ))
        .unwrap_or_default();
        for name in streams.iter().filter(|name| {
            apple::Stream {
                biome: &biome,
                name,
            }
            .is_raw()
        }) {
            if let Some(rows) = tri!(soft(
                name,
                apple::Stream {
                    biome: &biome,
                    name,
                }
                .media(&mut cursors),
            )) {
                payload.media.extend(rows);
            }
        }
        next.biome = cursors;

        next.catalog = tri!(serde_json::to_string(&(&devices, &streams)));
        if next.catalog != state.catalog {
            payload.devices = devices;
            payload.streams = streams;
        }
        Ok(())
    }

    /// Read new Safari, Firefox and Orion visits, up to `BROWSER_CAP` in all.
    fn collect_browsers(
        self,
        state: &State,
        next: &mut State,
        payload: &mut Payload,
    ) -> Result<()> {
        let mut visits = Vec::new();
        if let Some(rows) = tri!(soft(
            "Safari",
            browser::Safari {
                db: &self.home.join("Library/Safari/History.db"),
            }
            .visits(state.safari, BROWSER_CAP),
        )) {
            next.safari = rows.last().map_or(state.safari, |visit| visit.visit_id);
            visits.extend(rows);
        }
        let profiles = self
            .home
            .join("Library/Application Support/Firefox/Profiles");
        let firefox = browser::Firefox {
            profiles: &profiles,
        };
        for profile in tri!(soft("Firefox", firefox.profiles())).unwrap_or_default() {
            let left = BROWSER_CAP.saturating_sub(visits.len());
            if left == 0 {
                break;
            }
            let since = state.firefox.get(&profile).copied().unwrap_or(0);
            let what = format!("Firefox {profile}");
            if let Some(rows) = tri!(soft(&what, firefox.visits(&profile, since, left))) {
                let _: Option<i64> = next
                    .firefox
                    .insert(profile, rows.last().map_or(since, |visit| visit.visit_id));
                visits.extend(rows);
            }
        }
        let left = BROWSER_CAP.saturating_sub(visits.len());
        if left > 0
            && let Some(rows) = tri!(soft(
                "Orion",
                browser::Orion {
                    db: &self
                        .home
                        .join("Library/Application Support/Orion/Defaults/history"),
                }
                .visits(state.orion, left),
            ))
        {
            next.orion = rows.last().map_or(state.orion, |visit| visit.visit_id);
            visits.extend(rows);
        }
        payload.browser = visits;
        Ok(())
    }

    /// Read new Claude Code events.
    fn collect_claude(self, next: &mut State, payload: &mut Payload) -> Result<()> {
        let mut offsets = next.claude.clone();
        if let Some(rows) = tri!(soft(
            "Claude Code",
            claude::Projects {
                dir: &self.home.join(".claude/projects"),
            }
            .events(&mut offsets, CLAUDE_BUDGET),
        )) {
            next.claude = offsets;
            payload.claude = rows;
        }
        Ok(())
    }

    /// Read changed and deleted Obsidian notes.
    fn collect_obsidian(
        self,
        state: &State,
        next: &mut State,
        payload: &mut Payload,
    ) -> Result<()> {
        if let Some((rows, sent)) = tri!(soft(
            "Obsidian",
            obsidian::Vault {
                dir: &self.home.join(OBSIDIAN_VAULT),
            }
            .notes(&state.obsidian, OBSIDIAN_CAP),
        )) {
            next.obsidian = sent;
            payload.obsidian = rows;
        }
        Ok(())
    }

    /// Read changed Photos assets and labels.
    fn collect_photos(self, state: &State, next: &mut State, payload: &mut Payload) -> Result<()> {
        let library = photos::Photos {
            db: &self
                .home
                .join("Pictures/Photos Library.photoslibrary/database/Photos.sqlite"),
        };
        if let Some((rows, cursor)) =
            tri!(soft("Photos", library.assets(&state.photos, PHOTOS_CAP),))
        {
            next.photos = cursor;
            payload.photos = rows;
        }
        if let Some((rows, sent)) = tri!(soft(
            "Photos labels",
            library.labels(&state.photo_labels, PHOTOS_CAP),
        )) {
            next.photo_labels = sent;
            payload.photo_labels = rows;
        }
        Ok(())
    }

    /// Read changed Podcasts episodes.
    fn collect_podcasts(
        self,
        state: &State,
        next: &mut State,
        payload: &mut Payload,
    ) -> Result<()> {
        if let Some((rows, sent)) = tri!(soft(
            "Podcasts",
            podcasts::Podcasts {
                db: &self.home.join(PODCASTS_DB),
            }
            .episodes(&state.podcasts, PODCASTS_CAP),
        )) {
            next.podcasts = sent;
            payload.podcasts = rows;
        }
        Ok(())
    }

    /// Read new zsh and fish commands.
    fn collect_shells(self, state: &State, next: &mut State, payload: &mut Payload) -> Result<()> {
        let histories = [
            History {
                path: self.home.join(".zsh_history"),
                shell: Shell::Zsh,
            },
            History {
                path: self.home.join(".local/share/fish/fish_history"),
                shell: Shell::Fish,
            },
        ];
        for history in histories {
            let name = history.shell.name();
            if let Some(entries) = tri!(soft(name, history.entries())) {
                let since = state.shell.get(name).cloned().unwrap_or_default();
                let (rows, cursor) = history.new_commands(entries, &since, SHELL_CAP);
                if cursor != since {
                    drop(next.shell.insert(name.to_owned(), cursor));
                }
                payload.shell.extend(rows);
            }
        }
        Ok(())
    }

    /// One `--flights` poll; errors are logged, as launchd runs this every minute and a missed poll costs nothing.
    fn poll_flights(self) -> ExitCode {
        let support = self.support();
        match config(&support.join("config.json")).and_then(|cfg| cfg.poll(&support)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                log(&format!("flights: {error:#}"));
                ExitCode::FAILURE
            }
        }
    }

    /// One run: read what is new, push it as `host`, then move the cursors.
    fn run(self, host: &str) -> Result<()> {
        let support = self.support();
        let cfg = tri!(config(&support.join("config.json")));
        let state_path = support.join("state.json");
        let state = tri!(State::try_from(state_path.as_path()));
        let (mut payload, next) = tri!(self.collect(host, &state));
        let flights = Flights { support: &support };
        payload.flights = tri!(flights.take());
        if payload.is_empty() {
            log("nothing new");
            return Ok(());
        }
        let body = tri!(serde_json::to_string(&payload));
        tri!(cfg.push(&body));
        tri!(next.save(&state_path));
        tri!(flights.sent());
        log(&format!(
            "sent {} mac usage, {} focus, {} media, {} claude, {} browser, {} photos, {} photo labels, {} podcasts, {} obsidian, {} shell, {} flight polls, {} bytes",
            payload.mac_usage.len(),
            payload.focus.len(),
            payload.media.len(),
            payload.claude.len(),
            payload.browser.len(),
            payload.photos.len(),
            payload.photo_labels.len(),
            payload.podcasts.len(),
            payload.obsidian.len(),
            payload.shell.len(),
            payload.flights.len(),
            body.len()
        ));
        Ok(())
    }

    /// The directory that holds the config, the state and the flight polls.
    fn support(self) -> PathBuf {
        self.home
            .join("Library/Application Support/personal-data-mac")
    }
}

/// One push to the collector.
#[derive(Default)]
struct Payload {
    /// New Safari, Firefox and Orion visits.
    browser: Vec<browser::HistoryVisit>,
    /// New Claude Code events.
    claude: Vec<claude::Event>,
    /// The Biome sync peers, when they changed.
    devices: Vec<apple::Device>,
    /// The aircraft polls since the last push.
    flights: Vec<Value>,
    /// New app focus records of the other devices.
    focus: Vec<apple::Focus>,
    /// This Mac's name.
    host: String,
    /// New knowledgeC app usage rows.
    mac_usage: Vec<apple::MacUsage>,
    /// New Biome media and context records.
    media: Vec<apple::Media>,
    /// Changed and deleted Obsidian notes.
    obsidian: Vec<Value>,
    /// Changed Photos labels.
    photo_labels: Vec<Value>,
    /// Changed Photos assets.
    photos: Vec<Value>,
    /// Changed Podcasts episodes.
    podcasts: Vec<Value>,
    /// When the push was built.
    sent_at: String,
    /// New shell commands.
    shell: Vec<shell::Command>,
    /// The Biome streams present, when they changed.
    streams: Vec<String>,
}

impl Payload {
    /// Whether there is nothing to push.
    const fn is_empty(&self) -> bool {
        self.mac_usage.is_empty()
            && self.devices.is_empty()
            && self.streams.is_empty()
            && self.focus.is_empty()
            && self.media.is_empty()
            && self.claude.is_empty()
            && self.browser.is_empty()
            && self.photos.is_empty()
            && self.photo_labels.is_empty()
            && self.podcasts.is_empty()
            && self.obsidian.is_empty()
            && self.shell.is_empty()
            && self.flights.is_empty()
    }
}

impl Serialize for Payload {
    /// Emit the fields in the order the collector has always received them.
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut payload = tri!(serializer.serialize_struct("Payload", 15));
        tri!(payload.serialize_field("host", &self.host));
        tri!(payload.serialize_field("sent_at", &self.sent_at));
        tri!(payload.serialize_field("mac_usage", &self.mac_usage));
        tri!(payload.serialize_field("focus", &self.focus));
        tri!(payload.serialize_field("devices", &self.devices));
        tri!(payload.serialize_field("streams", &self.streams));
        tri!(payload.serialize_field("media", &self.media));
        tri!(payload.serialize_field("claude", &self.claude));
        tri!(payload.serialize_field("browser", &self.browser));
        tri!(payload.serialize_field("photos", &self.photos));
        tri!(payload.serialize_field("photo_labels", &self.photo_labels));
        tri!(payload.serialize_field("podcasts", &self.podcasts));
        tri!(payload.serialize_field("obsidian", &self.obsidian));
        tri!(payload.serialize_field("shell", &self.shell));
        tri!(payload.serialize_field("flights", &self.flights));
        payload.end()
    }
}

/// The path given to `--probe`, `--dump` or `--segb`.
#[derive(Clone, Copy, Debug)]
struct Probe<'path> {
    /// The directory or file to look at.
    path: &'path Path,
}

impl Probe<'_> {
    /// Print the first rows of every table of a `SQLite` database, each value cut to 300 characters.
    fn dump(self) -> Result<()> {
        let copy = tri!(apple::open_copy(self.path));
        let mut names = tri!(
            copy.conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        );
        let tables: Vec<String> =
            tri!(tri!(names.query_map([], |row| row.get(0))).collect::<rusqlite::Result<_>>());
        for table in tables {
            out(&format!("== {table}"));
            let mut select = tri!(
                copy.conn
                    .prepare(&format!("SELECT * FROM \"{table}\" LIMIT 60"))
            );
            let columns = select.column_count();
            let mut rows = tri!(select.query([]));
            while let Some(row) = tri!(rows.next()) {
                let values: Vec<String> = (0..columns)
                    .map(|index| match row.get_ref(index) {
                        Ok(ValueRef::Null) => "Null".to_owned(),
                        Ok(ValueRef::Integer(int)) => format!("Integer({int})"),
                        Ok(ValueRef::Real(real)) => format!("Real({real})"),
                        Ok(ValueRef::Text(bytes)) => {
                            String::from_utf8_lossy(bytes).chars().take(300).collect()
                        }
                        Ok(ValueRef::Blob(bytes)) => {
                            let text = String::from_utf8_lossy(bytes);
                            format!(
                                "blob[{}]:{}",
                                bytes.len(),
                                text.chars().take(300).collect::<String>()
                            )
                        }
                        Err(error) => error.to_string(),
                    })
                    .collect();
                out(&format!("  {}", values.join(" | ")));
            }
        }
        Ok(())
    }

    /// Print what the SEGB reader makes of a segment file: header, records per state, and the newest record times.
    fn segb(self) -> Result<()> {
        let bytes = tri!(fs::read(self.path));
        let version = if bytes.get(52..56) == Some(b"SEGB") {
            "v1"
        } else {
            "v2"
        };
        let header = tri!(le_i32(&bytes, 4));
        out(&format!(
            "{} bytes, {version}, header count {header}",
            bytes.len()
        ));
        let records = tri!(segb::Segment { bytes: &bytes }.records());
        let mut states = BTreeMap::new();
        for record in &records {
            let count: &mut u32 = states.entry((record.state, record.crc_ok)).or_insert(0_u32);
            *count = count.saturating_add(1);
        }
        let summary: Vec<String> = states
            .iter()
            .map(|(&(state, crc_ok), count)| format!("({state}, {crc_ok}): {count}"))
            .collect();
        out(&format!(
            "records by (state, crc ok): {{{}}}",
            summary.join(", ")
        ));
        let mut newest: Vec<&segb::Record> = records.iter().collect();
        newest.sort_by(|left, right| right.time.total_cmp(&left.time));
        for record in newest.iter().take(12) {
            out(&format!(
                "  {} state {} crc {} {} bytes",
                apple::iso(record.time),
                record.state,
                record.crc_ok,
                record.data.len()
            ));
        }
        if version == "v2" {
            // The raw trailer, newest slots included, to compare with what the reader kept.
            let count = usize::try_from(header).unwrap_or(0);
            let trailer = bytes.len().saturating_sub(count.saturating_mul(16));
            for index in (0..count).rev().take(5) {
                let at = trailer.saturating_add(index.saturating_mul(16));
                let end = tri!(le_i32(&bytes, at));
                let state = tri!(le_i32(&bytes, at.saturating_add(4)));
                let time = tri!(le_f64(&bytes, at.saturating_add(8)));
                out(&format!(
                    "  slot {index}: end {end} state {state} time {}",
                    apple::iso(time)
                ));
            }
            for index in 0..count.min(3) {
                let at = bytes
                    .len()
                    .saturating_sub(index.saturating_add(1).saturating_mul(16));
                let end = tri!(le_i32(&bytes, at));
                let state = tri!(le_i32(&bytes, at.saturating_add(4)));
                let time = tri!(le_f64(&bytes, at.saturating_add(8)));
                out(&format!(
                    "  from end {index}: end {end} state {state} time {}",
                    apple::iso(time)
                ));
            }
        }
        Ok(())
    }

    /// Print every file under the path, and the tables, row counts and columns of each `SQLite` database.
    ///
    /// Run it through launchd, which applies the Full Disk Access grant, to find what an app keeps in its container.
    fn tree(self, depth: usize) -> Result<()> {
        for entry in tri!(apple::list(self.path)) {
            let path = entry.path();
            if tri!(entry.file_type()).is_dir() {
                if let Some(left) = depth.checked_sub(1) {
                    tri!(Probe { path: &path }.tree(left));
                }
                continue;
            }
            out(&format!(
                "{}\t{}",
                path.display(),
                tri!(entry.metadata()).len()
            ));
            let name = path.to_string_lossy();
            if !(name.ends_with(".sqlite") || name.ends_with(".db") || name.ends_with(".sqlite3")) {
                continue;
            }
            let Ok(copy) = apple::open_copy(&path) else {
                continue;
            };
            let mut names = tri!(
                copy.conn
                    .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            );
            let tables: Vec<String> =
                tri!(tri!(names.query_map([], |row| row.get(0))).collect::<rusqlite::Result<_>>());
            for table in tables {
                let count: i64 = tri!(copy.conn.query_row(
                    &format!("SELECT count(*) FROM \"{table}\""),
                    [],
                    |row| row.get(0)
                ));
                let cols = tri!(browser::columns(&copy.conn, &table)).join(",");
                out(&format!("  {table}\t{count}\t{cols}"));
            }
        }
        Ok(())
    }
}

/// Cursors, moved only after the collector accepted a push.
#[derive(Default, Clone, Deserialize, PartialEq, Debug)]
struct State {
    /// Newest record time sent per Biome `<stream>/<device>`, in seconds since 2001.
    #[serde(default)]
    biome: BTreeMap<String, f64>,
    /// The devices and streams last sent, which are sent again only when they change.
    #[serde(default)]
    catalog: String,
    /// Byte offset read per Claude Code session file.
    #[serde(default)]
    claude: BTreeMap<String, u64>,
    /// Newest `moz_historyvisits.id` sent per Firefox profile directory.
    #[serde(default)]
    firefox: BTreeMap<String, i64>,
    /// Newest knowledgeC `ZCREATIONDATE` sent, in seconds since 2001.
    #[serde(default)]
    mac_created: f64,
    /// Rows sent that knowledgeC returns again within the overlap.
    #[serde(default)]
    mac_seen: BTreeSet<String>,
    /// The fields last sent per Obsidian note.
    #[serde(default)]
    obsidian: obsidian::Sent,
    /// Newest Orion `visits.ID` sent.
    #[serde(default)]
    orion: i64,
    /// The labels last sent per Photos asset.
    #[serde(default)]
    photo_labels: photos::Sent,
    /// The last Photos asset sent, by modification time and uuid.
    #[serde(default)]
    photos: photos::Cursor,
    /// The fields last sent per Podcasts episode.
    #[serde(default)]
    podcasts: podcasts::Sent,
    /// Newest Safari `history_visits.id` sent.
    #[serde(default)]
    safari: i64,
    /// Newest command time sent per shell, with the hashes of the commands at that second.
    #[serde(default)]
    shell: BTreeMap<String, shell::Cursor>,
}

impl State {
    /// Write the state to `path`, whole or not at all.
    fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("json.tmp");
        tri!(fs::write(&tmp, tri!(serde_json::to_string_pretty(self))));
        tri!(fs::rename(&tmp, path));
        Ok(())
    }
}

impl Serialize for State {
    /// Emit the fields in the order the state file has always had them.
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("State", 13));
        tri!(state.serialize_field("mac_created", &self.mac_created));
        tri!(state.serialize_field("mac_seen", &self.mac_seen));
        tri!(state.serialize_field("catalog", &self.catalog));
        tri!(state.serialize_field("biome", &self.biome));
        tri!(state.serialize_field("claude", &self.claude));
        tri!(state.serialize_field("safari", &self.safari));
        tri!(state.serialize_field("firefox", &self.firefox));
        tri!(state.serialize_field("orion", &self.orion));
        tri!(state.serialize_field("photos", &self.photos));
        tri!(state.serialize_field("photo_labels", &self.photo_labels));
        tri!(state.serialize_field("podcasts", &self.podcasts));
        tri!(state.serialize_field("obsidian", &self.obsidian));
        tri!(state.serialize_field("shell", &self.shell));
        state.end()
    }
}

impl TryFrom<&Path> for State {
    type Error = anyhow::Error;

    /// Read the state at `path`; none yet is the default.
    fn try_from(path: &Path) -> Result<Self> {
        match fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text)
                .with_context(|| format!("{} is not valid", path.display())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
        }
    }
}

/// Read the config, or create a template and fail when there is none.
fn config(path: &Path) -> Result<Config> {
    match fs::read_to_string(path) {
        Ok(text) => {
            let cfg: Config = tri!(
                serde_json::from_str(&text)
                    .with_context(|| format!("{} is not valid config JSON", path.display()))
            );
            if cfg.secret.trim().is_empty() {
                bail!("set \"secret\" in {} to SCREEN_TIME_SECRET", path.display());
            }
            Ok(cfg)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tri!(fs::create_dir_all(tri!(
                path.parent().context("config path has no parent")
            )));
            let template = Config {
                flights: None,
                secret: String::new(),
                url: DEFAULT_URL.into(),
            };
            let mut json = tri!(serde_json::to_string_pretty(&template));
            json.push('\n');
            tri!(fs::write(path, json));
            bail!(
                "created {}: set \"secret\" to SCREEN_TIME_SECRET and run again",
                path.display()
            )
        }
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

/// The little-endian `f64` at `at` in `bytes`.
fn le_f64(bytes: &[u8], at: usize) -> Result<f64> {
    let end = tri!(at.checked_add(8).context("offset overflow"));
    let slice = tri!(
        bytes
            .get(at..end)
            .with_context(|| format!("no f64 at {at}"))
    );
    Ok(f64::from_le_bytes(tri!(slice.try_into())))
}

/// The little-endian `i32` at `at` in `bytes`.
fn le_i32(bytes: &[u8], at: usize) -> Result<i32> {
    let end = tri!(at.checked_add(4).context("offset overflow"));
    let slice = tri!(
        bytes
            .get(at..end)
            .with_context(|| format!("no i32 at {at}"))
    );
    Ok(i32::from_le_bytes(tri!(slice.try_into())))
}

/// Print a timestamped line.
fn log(msg: &str) {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    out(&format!("{now} {msg}"));
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(String::as_str);
    if let (Some(flag @ ("--probe" | "--dump" | "--segb")), Some(path), None) =
        (mode, args.get(2), args.get(3))
    {
        let probe = Probe {
            path: Path::new(path),
        };
        let result = match flag {
            "--probe" => probe.tree(6),
            "--dump" => probe.dump(),
            _ => probe.segb(),
        };
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                log(&format!("probe: {error:#}"));
                ExitCode::FAILURE
            }
        };
    }
    let Some(home) = env::var_os("HOME").map(PathBuf::from) else {
        log("HOME is not set");
        return ExitCode::FAILURE;
    };
    let mac = Mac { home: &home };
    if mode == Some("--flights") {
        return mac.poll_flights();
    }
    // This Mac's name, as in System Settings, Sharing.
    let host = Command::new("scutil")
        .args(["--get", "ComputerName"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "Mac".into());
    match mac.run(&host) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if needs_disk_access(&error) => {
            let exe = env::current_exe().map_or_else(
                |_| "personal-data-mac".into(),
                |path| path.display().to_string(),
            );
            log(&format!(
                "Operation not permitted: grant Full Disk Access to {exe} in System Settings, \
                 Privacy & Security, Full Disk Access, then run again ({error:#})"
            ));
            ExitCode::from(2)
        }
        Err(error) => {
            log(&format!("error: {error:#}"));
            ExitCode::FAILURE
        }
    }
}

/// Whether an error is macOS refusing access, which Full Disk Access fixes.
fn needs_disk_access(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|io_error| io_error.kind() == io::ErrorKind::PermissionDenied)
    })
}

/// Write one line to stdout; a closed stdout is not worth failing a run over.
fn out(line: &str) {
    drop(writeln!(io::stdout(), "{line}"));
}

/// Log a failed part and go on without it, except a missing Full Disk Access, which fails the run.
fn soft<T>(what: &str, result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if needs_disk_access(&error) => Err(error),
        Err(error) => {
            log(&format!("{what}: {error:#}"));
            Ok(None)
        }
    }
}
