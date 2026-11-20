//! Tests for `shell`.

use alloc::collections::BTreeSet;
use std::env::temp_dir;
use std::fs;
use std::process;

use anyhow::{Result, ensure};

use super::{Command, Entry, History, Shell};
use crate::tri;

/// The commands of `sent`, in order.
fn words(sent: &[Command]) -> Vec<&str> {
    sent.iter().map(|command| command.word.as_str()).collect()
}

/// Keep only the command of a line.
#[test]
fn reduces_a_line_to_its_command() -> Result<()> {
    let first_word = |line: &str| {
        Entry {
            duration_s: None,
            line: line.to_owned(),
            when: 0,
        }
        .command()
    };
    ensure!(
        first_word("git status").as_deref() == Some("git"),
        "a plain command"
    );
    ensure!(
        first_word("FOO=bar BAZ=1 /opt/homebrew/bin/brew install fish").as_deref() == Some("brew"),
        "assignments and a path are dropped"
    );
    ensure!(
        first_word("sudo -i rm -rf /").as_deref() == Some("rm"),
        "sudo and its flags are dropped"
    );
    ensure!(
        first_word("time cargo test").as_deref() == Some("cargo"),
        "time is dropped"
    );
    ensure!(
        first_word("\\ls -la").as_deref() == Some("ls"),
        "a backslash is dropped"
    );
    ensure!(
        first_word("(cd x && make)").as_deref() == Some("cd"),
        "a subshell paren is dropped"
    );
    ensure!(
        first_word("./run.sh now").as_deref() == Some("run.sh"),
        "a relative path is dropped"
    );
    ensure!(
        first_word("cd;").as_deref() == Some("cd"),
        "a semicolon is dropped"
    );
    ensure!(
        first_word("TOKEN=secret").is_none(),
        "an assignment alone is no command"
    );
    ensure!(first_word("   ").is_none(), "blank is no command");
    Ok(())
}

/// Join zsh continuation lines and skip lines without a time.
#[test]
fn parses_zsh_with_continuations() -> Result<()> {
    let dir = temp_dir().join(format!("personal-data-mac-zsh-{}", process::id()));
    drop(fs::remove_dir_all(&dir));
    tri!(fs::create_dir_all(&dir));
    let path = dir.join(".zsh_history");
    tri!(fs::write(
        &path,
        ": 1697888323:0;git -v\n: 1697888330:12;cargo build \\\n  --release\n\
         plain line without a time\n: 1697888340:0;API_KEY=hunter2 curl https://x\n",
    ));
    let history = History {
        path,
        shell: Shell::Zsh,
    };
    let entries = tri!(history.entries());
    ensure!(entries.len() == 3, "three timed entries");
    ensure!(
        entries.get(1).map(|entry| entry.line.as_str()) == Some("cargo build \\\n  --release"),
        "the continuation is joined"
    );
    ensure!(
        entries.get(1).and_then(|entry| entry.duration_s) == Some(12),
        "the duration is kept"
    );

    let (sent, cursor) = history.new_commands(entries, &(0_i64, BTreeSet::new()), 100);
    ensure!(
        words(&sent) == ["git", "cargo", "curl"],
        "every command is sent"
    );
    ensure!(
        sent.first().map(|command| command.time.as_str()) == Some("2023-10-21T11:38:43Z"),
        "the time is RFC 3339"
    );
    ensure!(
        sent.get(1).and_then(|command| command.duration_s) == Some(12),
        "the duration is sent"
    );
    ensure!(
        sent.get(2).and_then(|command| command.duration_s) == Some(0),
        "a zero duration is sent"
    );
    ensure!(cursor.0 == 1_697_888_340, "the cursor is the newest time");
    ensure!(cursor.1.len() == 1, "one entry at that time");
    let missing = History {
        path: dir.join("missing"),
        shell: Shell::Zsh,
    };
    ensure!(
        tri!(missing.entries()).is_empty(),
        "a missing file has no entries"
    );
    Ok(())
}

/// Parse fish and send nothing twice across runs.
#[test]
fn parses_fish_and_resumes_from_the_cursor() -> Result<()> {
    let dir = temp_dir().join(format!("personal-data-mac-fish-{}", process::id()));
    drop(fs::remove_dir_all(&dir));
    tri!(fs::create_dir_all(&dir));
    let path = dir.join("fish_history");
    tri!(fs::write(
        &path,
        "- cmd: brew install fish\n  when: 1701030424\n  paths:\n    - /opt/x\n\
         - cmd: zsh\n  when: 1707235887\n- cmd: ls\n  when: 1707235887\n",
    ));
    let history = History {
        path,
        shell: Shell::Fish,
    };
    let entries = tri!(history.entries());
    ensure!(entries.len() == 3, "three entries");
    ensure!(
        entries.first().map(|entry| entry.line.as_str()) == Some("brew install fish"),
        "the whole line is kept"
    );

    let (first, cursor) =
        history.new_commands(tri!(history.entries()), &(0_i64, BTreeSet::new()), 2);
    ensure!(words(&first) == ["brew", "zsh"], "the limit cuts the run");
    ensure!(
        cursor.0 == 1_707_235_887,
        "the cursor is the newest time sent"
    );
    // The next run sends the other entry of that second and nothing twice, even after a rewrite reorders the file.
    let (second, next) = history.new_commands(tri!(history.entries()), &cursor, 100);
    ensure!(
        words(&second) == ["ls"],
        "only the other entry of that second"
    );
    ensure!(
        next.1.len() == 2,
        "both entries of that second are in the cursor"
    );
    let (third, _) = history.new_commands(tri!(history.entries()), &next, 100);
    ensure!(third.is_empty(), "nothing is sent twice");
    Ok(())
}
