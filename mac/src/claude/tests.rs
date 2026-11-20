//! Tests for `claude`.

use alloc::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::{env, process};

use anyhow::{Context as _, Result, ensure};
use serde_json::json;

use super::Projects;
use crate::tri;

/// A secret that must never leave the session file.
const SECRET: &str = "sk-very-secret";

/// Read kinds, tools and tokens but no text, then only the line completed since the last run.
#[test]
fn keeps_metadata_only() -> Result<()> {
    let dir = env::temp_dir().join(format!("personal-data-mac-claude-{}", process::id()));
    if dir.exists() {
        tri!(fs::remove_dir_all(&dir));
    }
    tri!(fs::create_dir_all(dir.join("-Users-alex-proj")));
    let file = dir.join("-Users-alex-proj/s1.jsonl");
    let mut partial = lines();
    partial.push_str(r#"{"type": "user", "timestamp": "2026"#);
    tri!(fs::write(&file, partial));
    let mut offsets = BTreeMap::new();
    let projects = Projects { dir: &dir };
    let events = tri!(projects.events(&mut offsets, u64::MAX));
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
    ensure!(kinds == ["user", "tool_use", "tool_result"], "the kinds");
    let second = tri!(events.get(1).context("no second event"));
    ensure!(second.tool.as_deref() == Some("Bash"), "the tool");
    ensure!(second.output_tokens == Some(20), "the tokens");
    ensure!(
        events.first().map(|event| event.project.as_str()) == Some("-Users-alex-proj"),
        "the project"
    );
    let sent = tri!(serde_json::to_string(&events));
    ensure!(!sent.contains(SECRET), "{sent}");
    // The partial last line waits; once complete, only it is read.
    let key = file.to_string_lossy().into_owned();
    ensure!(
        offsets.get(&key).copied() == Some(tri!(u64::try_from(lines().len()))),
        "the offset stops before the partial line"
    );
    let mut appender = tri!(OpenOptions::new().append(true).open(&file));
    tri!(appender.write_all(b"-10-16T11:00:00Z\"}\n"));
    let more = tri!(projects.events(&mut offsets, u64::MAX));
    ensure!(more.len() == 1, "only the completed line");
    ensure!(
        more.first().map(|event| event.ts.as_str()) == Some("2026-10-16T11:00:00Z"),
        "its time"
    );
    Ok(())
}

/// Four session lines, each holding the secret somewhere outside the metadata.
fn lines() -> String {
    let mut text = String::new();
    for line in [
        json!({"type": "user", "sessionId": "s1", "uuid": "u1", "timestamp": "2026-10-16T10:00:00.000Z",
               "message": {"role": "user", "content": format!("my key is {SECRET}")}}),
        json!({"type": "assistant", "sessionId": "s1", "uuid": "u2", "timestamp": "2026-10-16T10:00:05.000Z",
               "message": {"id": "m1", "model": "claude-opus-5-5",
                           "content": [{"type": "tool_use", "name": "Bash", "input": {"command": SECRET}}],
                           "usage": {"input_tokens": 10_u64, "output_tokens": 20_u64,
                                     "cache_read_input_tokens": 30_u64}}}),
        json!({"type": "user", "sessionId": "s1", "uuid": "u3", "timestamp": "2026-10-16T10:00:06.000Z",
               "toolUseResult": {"stdout": SECRET},
               "message": {"content": [{"type": "tool_result", "content": SECRET}]}}),
        json!({"type": "summary", "summary": SECRET}),
    ] {
        text.push_str(&line.to_string());
        text.push('\n');
    }
    text
}
