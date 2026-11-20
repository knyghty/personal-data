//! Tests for the run: collecting, pushing and moving the cursors.

use std::env::temp_dir;
use std::fs;
use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::process;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use anyhow::{Error, Result, bail, ensure};
use serde_json::{Value, json};

use super::{BROWSER_CAP, DEFAULT_URL, Mac, State, config, needs_disk_access, soft};
use crate::apple::tests::{knowledge_db, sync_db, transition};
use crate::browser::tests::{places_db, safari_db};
use crate::segb::tests::segb_v2;
use crate::tri;

/// Answer one request with `status` and send its authorization header and body back.
fn fake_collector(status: u16) -> Result<(String, Receiver<(String, String)>)> {
    let listener = tri!(TcpListener::bind("127.0.0.1:0"));
    let addr = tri!(listener.local_addr());
    let url = format!("http://{addr}/screen-time/ingest");
    let (sender, receiver) = mpsc::channel();
    drop(thread::spawn(move || -> Result<()> {
        let (mut stream, _) = tri!(listener.accept());
        let mut reader = BufReader::new(tri!(stream.try_clone()));
        let (mut len, mut auth) = (0_usize, String::new());
        loop {
            let mut line = String::new();
            let _: usize = tri!(reader.read_line(&mut line));
            let lower = line.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("content-length:") {
                len = tri!(value.trim().parse());
            }
            if lower.starts_with("authorization:")
                && let Some((_, value)) = line.split_once(':')
            {
                auth = value.trim().to_owned();
            }
            if line == "\r\n" {
                break;
            }
        }
        let mut body = vec![0_u8; len];
        tri!(reader.read_exact(&mut body));
        let reply = "content-length: 2\r\nconnection: close\r\n\r\n{}";
        tri!(write!(stream, "HTTP/1.1 {status} X\r\n{reply}"));
        tri!(sender.send((auth, tri!(String::from_utf8(body)))));
        Ok(())
    }));
    Ok((url, receiver))
}

/// Send at most `BROWSER_CAP` visits per push, Safari first.
#[test]
fn caps_browser_visits_per_push() -> Result<()> {
    let home = temp_dir().join(format!("personal-data-mac-cap-{}", process::id()));
    drop(fs::remove_dir_all(&home));
    tri!(fs::create_dir_all(home.join("Library/Safari")));
    let safari = tri!(safari_db(&home.join("Library/Safari/History.db"), &[]));
    tri!(safari.execute_batch(&format!(
        "INSERT INTO history_items (url, visit_count) VALUES ('https://example.org/', 0);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i <= {BROWSER_CAP})
         INSERT INTO history_visits (history_item, visit_time) SELECT 1, i FROM n;"
    )));
    let profile = home.join("Library/Application Support/Firefox/Profiles/p.default");
    tri!(fs::create_dir_all(&profile));
    let _firefox = tri!(places_db(
        &profile.join("places.sqlite"),
        &[("https://example.org/", 1_i64, 0_i64)],
    ));

    let mac = Mac { home: &home };
    let (first, state) = tri!(mac.collect("Mac", &State::default()));
    ensure!(first.browser.len() == BROWSER_CAP, "the cap fills the push");
    let cap = tri!(i64::try_from(BROWSER_CAP));
    ensure!(state.safari == cap, "the Safari cursor moves to the cap");
    ensure!(state.firefox.is_empty(), "Firefox waits for the next run");
    let (second, next) = tri!(mac.collect("Mac", &state));
    let browsers: Vec<_> = second.browser.iter().map(|visit| visit.browser).collect();
    ensure!(
        browsers == ["safari", "firefox"],
        "the rest of Safari, then Firefox"
    );
    ensure!(
        next.firefox.get("p.default") == Some(&1),
        "the Firefox cursor moves"
    );
    let (third, _) = tri!(mac.collect("Mac", &next));
    ensure!(third.browser.is_empty(), "nothing is left");
    Ok(())
}

/// Write a config template and fail until the secret is set.
#[test]
fn creates_a_config_template_when_missing() -> Result<()> {
    let dir = temp_dir().join(format!("personal-data-mac-cfg-{}", process::id()));
    drop(fs::remove_dir_all(&dir));
    let path = dir.join("config.json");
    let Err(error) = config(&path) else {
        bail!("a missing config is an error");
    };
    let message = error.to_string();
    ensure!(message.contains("set \"secret\""), "{message}");
    ensure!(
        tri!(fs::read_to_string(&path)).contains(DEFAULT_URL),
        "the template has the default url"
    );
    ensure!(config(&path).is_err(), "an empty secret is an error");
    Ok(())
}

/// Fail the run on EPERM and go on without anything else.
#[test]
fn detects_missing_disk_access() -> Result<()> {
    // EPERM, which macOS returns without Full Disk Access.
    let denied = Error::from(io::Error::from_raw_os_error(1)).context("copy");
    ensure!(needs_disk_access(&denied), "EPERM needs Full Disk Access");
    ensure!(soft::<()>("x", Err(denied)).is_err(), "EPERM fails the run");
    let missing = Error::from(io::Error::from(io::ErrorKind::NotFound));
    ensure!(!needs_disk_access(&missing), "a missing file is not EPERM");
    ensure!(
        tri!(soft::<()>("x", Err(missing))).is_none(),
        "a missing file is skipped"
    );
    Ok(())
}

/// Move the cursors only after the collector accepts the push.
#[test]
fn moves_cursors_only_after_the_collector_accepts() -> Result<()> {
    let home = temp_dir().join(format!("personal-data-mac-home-{}", process::id()));
    drop(fs::remove_dir_all(&home));
    let knowledge = home.join("Library/Application Support/Knowledge");
    tri!(fs::create_dir_all(&knowledge));
    drop(tri!(knowledge_db(
        &knowledge.join("knowledgeC.db"),
        &[("com.apple.Safari", 1000.0_f64, 1600.0_f64, 1601.0_f64, None)],
    )));
    let biome = home.join("Library/Biome");
    tri!(fs::create_dir_all(biome.join("sync")));
    tri!(sync_db(
        &biome.join("sync/sync.db"),
        &[("PHONE", "Alex's iPhone", 2_i64)],
    ));
    let phone = biome.join("streams/restricted/App.InFocus/remote/PHONE");
    tri!(fs::create_dir_all(&phone));
    tri!(fs::write(
        phone.join("1"),
        segb_v2(&[
            transition(true, 2000.0, "com.burbn.instagram"),
            transition(false, 2060.0, "com.burbn.instagram"),
        ]),
    ));
    let project = home.join(".claude/projects/-Users-alex-x");
    tri!(fs::create_dir_all(&project));
    tri!(fs::write(
        project.join("s.jsonl"),
        "{\"type\": \"user\", \"sessionId\": \"s\", \"timestamp\": \"2026-10-16T10:00:00Z\"}\n",
    ));
    tri!(fs::create_dir_all(home.join("Library/Safari")));
    drop(tri!(safari_db(
        &home.join("Library/Safari/History.db"),
        &[("https://example.org/", 1000.0_f64, 0_i64)],
    )));
    tri!(fs::write(
        home.join(".zsh_history"),
        ": 1793528390:0;claude --resume abc\n",
    ));
    let vault = home.join("Library/Mobile Documents/iCloud~md~obsidian/Documents/Default");
    tri!(fs::create_dir_all(&vault));
    tri!(fs::write(vault.join("Plan.md"), "# Plan\n\nBuy a desk.\n"));
    let support = home.join("Library/Application Support/personal-data-mac");
    let state_path = support.join("state.json");
    tri!(fs::create_dir_all(&support));
    let configure = |url: String| -> Result<()> {
        let cfg = json!({"url": url, "secret": "s3cret"});
        tri!(fs::write(support.join("config.json"), cfg.to_string()));
        Ok(())
    };
    let mac = Mac { home: &home };

    let (rejecting_url, rejected) = tri!(fake_collector(500));
    tri!(configure(rejecting_url));
    ensure!(mac.run("Mac").is_err(), "a rejected push fails the run");
    drop(tri!(rejected.recv()));
    ensure!(!state_path.exists(), "no state after a rejected push");

    let (accepting_url, accepted) = tri!(fake_collector(200));
    tri!(configure(accepting_url));
    tri!(mac.run("Mac"));
    let (auth, body) = tri!(accepted.recv());
    ensure!(auth == "Bearer s3cret", "the secret is the bearer token");
    let sent: Value = tri!(serde_json::from_str(&body));
    let count = |field: &str| sent.pointer(field).and_then(Value::as_array).map(Vec::len);
    ensure!(count("/mac_usage") == Some(1), "one mac usage row");
    ensure!(count("/focus") == Some(2), "two focus rows");
    ensure!(count("/devices") == Some(2), "the Mac and the phone");
    ensure!(count("/claude") == Some(1), "one Claude Code event");
    ensure!(count("/obsidian") == Some(1), "one Obsidian note");
    ensure!(
        !body.contains("abc") && !body.contains("Buy a desk"),
        "arguments and note text never leave the Mac: {body}"
    );
    let state = tri!(State::try_from(state_path.as_path()));
    let saved = tri!(serde_json::to_value(&state));
    for (document, field, expected) in [
        (&sent, "/host", json!("Mac")),
        (&sent, "/focus/0/device_name", json!("Alex's iPhone")),
        (&sent, "/streams/0", json!("restricted/App.InFocus")),
        (&sent, "/browser/0/url", json!("https://example.org/")),
        (&sent, "/shell/0/command", json!("claude")),
        (&sent, "/obsidian/0/path", json!("Default/Plan.md")),
        (&sent, "/obsidian/0/words", json!(5_u64)),
        (&saved, "/mac_created", json!(1601.0_f64)),
        (&saved, "/biome/App.InFocus~1PHONE", json!(2060.0_f64)),
        (&saved, "/safari", json!(1_i64)),
        (&saved, "/shell/zsh/0", json!(1_793_528_390_i64)),
    ] {
        ensure!(
            document.pointer(field) == Some(&expected),
            "{field} is {expected}"
        );
    }

    // knowledgeC returns the row again within the overlap, but it is not sent twice.
    let (again, next) = tri!(mac.collect("Mac", &state));
    ensure!(again.is_empty(), "nothing is sent twice");
    ensure!(next == state, "the state stays");
    Ok(())
}
