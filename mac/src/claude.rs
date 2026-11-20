//! Read Claude Code activity from `~/.claude/projects/*/*.jsonl` as metadata only.
//!
//! Sessions hold message text, tool inputs, outputs and file contents, often with pasted secrets. Only the fields of
//! `Event` are read from each line; nothing else leaves the Mac.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;

use anyhow::{Context as _, Result};
use serde::ser::SerializeStruct as _;
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::browser::optional;
use crate::tri;

/// The metadata of one session line; `Serialize` keeps the fields in the order the collector has always received.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Event {
    /// `message.usage.cache_creation_input_tokens`.
    pub cache_creation_tokens: Option<u64>,
    /// `message.usage.cache_read_input_tokens`.
    pub cache_read_tokens: Option<u64>,
    /// `message.usage.input_tokens`.
    pub input_tokens: Option<u64>,
    /// The line's `type`, or `tool_use` / `tool_result` when the message holds one.
    pub kind: String,
    /// `message.id`.
    pub message_id: Option<String>,
    /// `message.model`.
    pub model: Option<String>,
    /// `message.usage.output_tokens`.
    pub output_tokens: Option<u64>,
    /// The project directory name under `~/.claude/projects`.
    pub project: String,
    /// `sessionId`.
    pub session_id: Option<String>,
    /// `isSidechain`.
    pub sidechain: Option<bool>,
    /// The names of the `tool_use` blocks, comma separated.
    pub tool: Option<String>,
    /// `timestamp`.
    pub ts: String,
    /// `uuid`.
    pub uuid: Option<String>,
}

/// The project a session file belongs to, by directory name.
struct Project<'name> {
    /// The directory name under `~/.claude/projects`.
    name: &'name str,
}

/// The `~/.claude/projects` directory, one subdirectory of session files per project.
pub(super) struct Projects<'dir> {
    /// `~/.claude/projects`.
    pub dir: &'dir Path,
}

impl Project<'_> {
    /// The metadata of one session line, or `None` for lines without a time.
    fn event(&self, line: &str) -> Option<Event> {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return None;
        };
        let Some(ts) = text(value.get("timestamp")) else {
            return None;
        };
        let null = Value::Null;
        let message = value.get("message").unwrap_or(&null);
        let blocks = message
            .get("content")
            .and_then(Value::as_array)
            .map_or_default(Vec::as_slice);
        let tools: Vec<&str> = blocks
            .iter()
            .filter(|block| block_is(block, "tool_use"))
            .filter_map(|block| block.get("name").and_then(Value::as_str))
            .collect();
        let kind = if !tools.is_empty() {
            "tool_use".to_owned()
        } else if blocks.iter().any(|block| block_is(block, "tool_result")) {
            "tool_result".to_owned()
        } else {
            text(value.get("type")).unwrap_or_default()
        };
        let usage = message.get("usage").unwrap_or(&null);
        Some(Event {
            cache_creation_tokens: usage
                .get("cache_creation_input_tokens")
                .and_then(Value::as_u64),
            cache_read_tokens: usage.get("cache_read_input_tokens").and_then(Value::as_u64),
            input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
            kind,
            message_id: text(message.get("id")),
            model: text(message.get("model")),
            output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
            project: self.name.to_owned(),
            session_id: text(value.get("sessionId")),
            sidechain: value.get("isSidechain").and_then(Value::as_bool),
            tool: (!tools.is_empty()).then(|| tools.join(",")),
            ts,
            uuid: text(value.get("uuid")),
        })
    }
}

impl Projects<'_> {
    /// Events of every session file past its byte offset in `offsets`, which moves to the last complete line.
    ///
    /// Stop taking new files once `budget` bytes are read, so a first run spreads the history over several pushes.
    ///
    /// # Errors
    ///
    /// Fails when a session file cannot be opened or read.
    pub(crate) fn events(
        &self,
        offsets: &mut BTreeMap<String, u64>,
        budget: u64,
    ) -> Result<Vec<Event>> {
        let mut files = Vec::new();
        for dir in fs::read_dir(self.dir).into_iter().flatten().flatten() {
            for entry in fs::read_dir(dir.path()).into_iter().flatten().flatten() {
                if entry.path().extension().is_some_and(|ext| ext == "jsonl") {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        let mut out = Vec::new();
        let mut read = 0_u64;
        for path in files {
            if read >= budget {
                break;
            }
            let key = path.to_string_lossy().into_owned();
            let len = tri!(fs::metadata(&path)).len();
            // A file shorter than its offset was rewritten: read it again.
            let start = offsets
                .get(&key)
                .copied()
                .filter(|&offset| offset <= len)
                .unwrap_or(0);
            if start == len {
                continue;
            }
            let mut file = tri!(File::open(&path).with_context(|| format!("open {key}")));
            let _: u64 = tri!(file.seek(SeekFrom::Start(start)));
            let mut buf = Vec::new();
            let _: usize = tri!(file.take(len.saturating_sub(start)).read_to_end(&mut buf));
            // Leave a line still being written for the next run.
            let Some(end) = buf
                .iter()
                .rposition(|&byte| byte == b'\n')
                .and_then(|at| at.checked_add(1))
            else {
                continue;
            };
            buf.truncate(end);
            let name = path
                .parent()
                .and_then(Path::file_name)
                .map_or_else(String::new, |dir_name| {
                    dir_name.to_string_lossy().into_owned()
                });
            let project = Project { name: &name };
            for line in String::from_utf8_lossy(&buf).lines() {
                out.extend(project.event(line));
            }
            let consumed = tri!(u64::try_from(end));
            read = read.saturating_add(consumed);
            let _: Option<u64> = offsets.insert(key, start.saturating_add(consumed));
        }
        Ok(out)
    }
}

impl Serialize for Event {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut state = tri!(serializer.serialize_struct("Event", 13));
        tri!(state.serialize_field("session_id", &self.session_id));
        tri!(state.serialize_field("project", &self.project));
        tri!(state.serialize_field("ts", &self.ts));
        tri!(state.serialize_field("type", &self.kind));
        tri!(state.serialize_field("uuid", &self.uuid));
        tri!(optional(&mut state, "message_id", self.message_id.as_ref()));
        tri!(optional(&mut state, "model", self.model.as_ref()));
        tri!(optional(&mut state, "tool", self.tool.as_ref()));
        tri!(optional(
            &mut state,
            "input_tokens",
            self.input_tokens.as_ref()
        ));
        tri!(optional(
            &mut state,
            "output_tokens",
            self.output_tokens.as_ref()
        ));
        tri!(optional(
            &mut state,
            "cache_read_tokens",
            self.cache_read_tokens.as_ref()
        ));
        tri!(optional(
            &mut state,
            "cache_creation_tokens",
            self.cache_creation_tokens.as_ref()
        ));
        tri!(optional(&mut state, "sidechain", self.sidechain.as_ref()));
        state.end()
    }
}

/// Whether a content block's `type` is `kind`.
fn block_is(block: &Value, kind: &str) -> bool {
    block.get("type").and_then(Value::as_str) == Some(kind)
}

/// The string in `value`, when it is one.
fn text(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(String::from)
}
