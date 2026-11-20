//! Read when commands were run in zsh and fish, as the time and the command's first word only. Arguments hold
//! secrets, paths and other people's names, so they never leave the Mac.
//!
//! Both shells rewrite their history files (deduplication, trimming), so a byte offset does not survive. The cursor
//! is the newest time sent and the hashes of the entries at that time.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, SecondsFormat};
use serde::ser::SerializeStruct as _;
use serde::{Serialize, Serializer};

use crate::tri;

/// Words that run another command: the word after them is the command.
const WRAPPERS: &[&str] = &[
    "sudo", "time", "nohup", "env", "exec", "command", "builtin", "noglob", "doas",
];

/// One command as sent.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Command {
    /// How long it ran, when the shell records it.
    pub duration_s: Option<i64>,
    /// `zsh` or `fish`.
    pub shell: &'static str,
    /// When it ran, in RFC 3339.
    pub time: String,
    /// The first word of the line, sent as `command`.
    pub word: String,
}

impl Serialize for Command {
    /// Emit the fields in the order the collector has always received them.
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let fields = self.duration_s.map_or(3, |_| 4);
        let mut command = tri!(serializer.serialize_struct("Command", fields));
        tri!(command.serialize_field("shell", &self.shell));
        tri!(command.serialize_field("time", &self.time));
        tri!(command.serialize_field("command", &self.word));
        if let Some(duration_s) = self.duration_s {
            tri!(command.serialize_field("duration_s", &duration_s));
        }
        command.end()
    }
}

/// The newest `when` sent and the CRCs of the entries at that second.
pub(super) type Cursor = (i64, BTreeSet<u32>);

/// One history entry before it is reduced to its first word.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Entry {
    /// How long it ran, when the shell records it.
    pub duration_s: Option<i64>,
    /// The command line.
    pub line: String,
    /// When it ran, in Unix seconds.
    pub when: i64,
}

impl Entry {
    /// The first word of the line, without a leading path, past `VAR=value` assignments and wrappers.
    pub(crate) fn command(&self) -> Option<String> {
        for word in self.line.split_whitespace() {
            let plain = word.trim_start_matches(['\\', '(', '{']);
            if plain.is_empty() || plain.starts_with('-') || WRAPPERS.contains(&plain) {
                continue;
            }
            let assignment = plain.split_once('=').is_some_and(|(name, _)| {
                !name.is_empty() && name.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
            });
            if assignment {
                continue;
            }
            let trimmed = plain.trim_end_matches([';', '|', '&']);
            let base = trimmed.rsplit('/').next().unwrap_or(trimmed);
            return (!base.is_empty()).then(|| base.to_owned());
        }
        None
    }

    /// A hash that tells the entries of one second apart.
    fn crc(&self) -> u32 {
        crc32fast::hash(format!("{}|{}", self.when, self.line).as_bytes())
    }
}

/// A shell's history file.
#[derive(Debug)]
pub(super) struct History {
    /// The file.
    pub path: PathBuf,
    /// The shell whose format it is in.
    pub shell: Shell,
}

impl History {
    /// Parse the file's entries; a missing file has none.
    pub(crate) fn entries(&self) -> Result<Vec<Entry>> {
        let text = match fs::read(&self.path) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        Ok(self.shell.parse(&text))
    }

    /// The commands of `entries` after `since`, oldest first, at most `limit`, and the cursor after them.
    #[must_use]
    pub(crate) fn new_commands(
        &self,
        mut entries: Vec<Entry>,
        since: &Cursor,
        limit: usize,
    ) -> (Vec<Command>, Cursor) {
        entries.sort_by_key(|entry| entry.when);
        let mut cursor = since.clone();
        let mut out = Vec::new();
        for entry in entries {
            if out.len() == limit {
                break;
            }
            let hash = entry.crc();
            if entry.when < since.0 || (entry.when == since.0 && since.1.contains(&hash)) {
                continue;
            }
            if entry.when > cursor.0 {
                cursor = (entry.when, BTreeSet::new());
            }
            let _: bool = cursor.1.insert(hash);
            let Some(command) = entry.command() else {
                continue;
            };
            out.push(Command {
                duration_s: entry.duration_s,
                shell: self.shell.name(),
                time: DateTime::from_timestamp(entry.when, 0)
                    .map_or_default(|date| date.to_rfc3339_opts(SecondsFormat::Secs, true)),
                word: command,
            });
        }
        (out, cursor)
    }
}

/// A shell and the format of its history file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Shell {
    /// fish: `- cmd: <command>` followed by `  when: <epoch>` and other indented keys.
    Fish,
    /// zsh with `EXTENDED_HISTORY`: `: <epoch>:<seconds>;<command>`, continued on the next line after a backslash.
    Zsh,
}

impl Shell {
    /// The name sent with each command and keyed in the state.
    #[must_use]
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Fish => "fish",
            Self::Zsh => "zsh",
        }
    }

    /// The entries of a history file in this shell's format; zsh lines without a time are skipped.
    fn parse(self, text: &str) -> Vec<Entry> {
        let mut out = Vec::new();
        match self {
            Self::Fish => {
                let mut pending: Option<String> = None;
                for line in text.lines() {
                    if let Some(command) = line.strip_prefix("- cmd: ") {
                        pending = Some(command.to_owned());
                        continue;
                    }
                    if let Some(stamp) = line.strip_prefix("  when: ")
                        && let (Some(command), Ok(when)) =
                            (pending.take(), stamp.trim().parse::<i64>())
                    {
                        out.push(Entry {
                            duration_s: None,
                            line: command,
                            when,
                        });
                    }
                }
            }
            Self::Zsh => {
                let mut current: Option<Entry> = None;
                for line in text.split('\n') {
                    if let Some(mut entry) = current.take() {
                        entry.line.push('\n');
                        entry.line.push_str(line);
                        if line.ends_with('\\') {
                            current = Some(entry);
                        } else {
                            out.push(entry);
                        }
                        continue;
                    }
                    let Some(rest) = line.strip_prefix(": ") else {
                        continue;
                    };
                    let Some((stamp, command)) = rest.split_once(';') else {
                        continue;
                    };
                    let Some((epoch, duration)) = stamp.split_once(':') else {
                        continue;
                    };
                    let Ok(when) = epoch.trim().parse::<i64>() else {
                        continue;
                    };
                    let entry = Entry {
                        duration_s: duration.trim().parse().ok(),
                        line: command.to_owned(),
                        when,
                    };
                    if command.ends_with('\\') {
                        current = Some(entry);
                    } else {
                        out.push(entry);
                    }
                }
                if let Some(entry) = current {
                    out.push(entry);
                }
            }
        }
        out
    }
}
