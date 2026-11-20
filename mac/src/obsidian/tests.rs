//! Tests for `obsidian`.

use std::env::temp_dir;
use std::fs;
use std::process;

use anyhow::{Context as _, Result, ensure};
use serde_json::{Value, json};

use super::{Sent, Text, Vault};
use crate::tri;

/// A note with front matter tags, inline tags, a heading and three kinds of wiki link.
const NOTE: &str = "---\ntitle: Plan\ntags:\n  - project\n  - \"#home\"\n---\n# Plan\n\nBuy a [[Desk|new desk]] and \
see [[Furniture#Chairs]] and [[Desk]]. #todo, not #1 or ##.\n";

/// The paths of `rows`, in order.
fn paths(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .filter_map(|row| row.get("path").and_then(Value::as_str))
        .collect()
}

/// Send each file once, again when it changes, as deleted when it is gone, and no more than the cap per push.
#[test]
fn sends_notes_once_until_they_change_or_go() -> Result<()> {
    let dir = temp_dir().join(format!("personal-data-mac-obsidian-{}", process::id()));
    drop(fs::remove_dir_all(&dir));
    tri!(fs::create_dir_all(dir.join("Default/Personal/Food")));
    tri!(fs::create_dir_all(dir.join(".obsidian/plugins")));
    tri!(fs::create_dir_all(dir.join(".trash")));
    tri!(fs::write(dir.join(".obsidian/app.json"), "{}"));
    tri!(fs::write(dir.join(".trash/Old.md"), "gone"));
    tri!(fs::write(dir.join("Default/.Hidden.md.icloud"), ""));
    tri!(fs::write(dir.join("Default/Plan.md"), NOTE));
    tri!(fs::write(
        dir.join("Default/Personal/Food/Soup.cook"),
        "Boil @water{1%l} with #salt.\n"
    ));
    tri!(fs::write(
        dir.join("Default/Personal/pic.png"),
        [0_u8, 159, 146, 150]
    ));

    let vault = Vault { dir: &dir };
    let (rows, sent) = tri!(vault.notes(&Sent::new(), 10));
    ensure!(
        paths(&rows)
            == [
                "Default/Personal/Food/Soup.cook",
                "Default/Personal/pic.png",
                "Default/Plan.md"
            ],
        "every file outside dot directories, sorted: {:?}",
        paths(&rows)
    );
    let plan = tri!(rows.get(2).context("no plan"));
    for (field, expected) in [
        ("folder", json!("Default")),
        ("name", json!("Plan")),
        ("ext", json!("md")),
        ("bytes", json!(NOTE.len())),
        ("words", json!(16_usize)),
        ("lines", json!(3_usize)),
        ("tags", json!(["home", "project", "todo"])),
        ("links", json!(["Desk", "Furniture"])),
    ] {
        ensure!(
            plan.get(field) == Some(&expected),
            "{field} is {expected}, not {:?}",
            plan.get(field)
        );
    }
    ensure!(
        plan.get("modified")
            .and_then(Value::as_str)
            .is_some_and(|time| time.ends_with('Z')),
        "modified is RFC 3339 in UTC"
    );
    let soup = tri!(rows.first().context("no soup"));
    ensure!(
        soup.get("words") == Some(&json!(4_usize)) && soup.get("tags") == Some(&json!([])),
        "a cook file has counts but no tags: {soup}"
    );
    let pic = tri!(rows.get(1).context("no picture"));
    ensure!(
        pic.get("words").is_some_and(Value::is_null) && pic.get("bytes") == Some(&json!(4_u64)),
        "a binary file has a size but no counts: {pic}"
    );
    ensure!(
        !tri!(serde_json::to_string(&rows)).contains("Buy a"),
        "the text never leaves the Mac"
    );
    ensure!(tri!(vault.notes(&sent, 10)).0.is_empty(), "unchanged");

    tri!(fs::write(dir.join("Default/Plan.md"), "# Plan\n\nDone.\n"));
    tri!(fs::remove_file(dir.join("Default/Personal/pic.png")));
    let (changed, after) = tri!(vault.notes(&sent, 10));
    ensure!(
        paths(&changed) == ["Default/Plan.md", "Default/Personal/pic.png"],
        "the changed note, then the deleted file: {:?}",
        paths(&changed)
    );
    ensure!(
        changed.get(1).and_then(|row| row.get("deleted")) == Some(&json!(true)),
        "the deleted file is marked"
    );
    ensure!(
        after.len() == 2 && !after.contains_key("Default/Personal/pic.png"),
        "the deleted file leaves the state"
    );
    ensure!(tri!(vault.notes(&after, 10)).0.is_empty(), "settled");

    let (capped, partial) = tri!(vault.notes(&Sent::new(), 1));
    ensure!(
        capped.len() == 1 && partial.len() == 1,
        "the cap leaves the rest for the next push"
    );

    let (none, same) = tri!(
        Vault {
            dir: &dir.join("missing")
        }
        .notes(&after, 10)
    );
    ensure!(
        none.is_empty() && same == after,
        "no vault, nothing sent and nothing forgotten"
    );
    Ok(())
}

/// Split the front matter off, and leave text without one whole.
#[test]
fn splits_front_matter() -> Result<()> {
    let split = |text: &'static str| Text(text).split_front_matter();
    ensure!(
        split("---\na: 1\n---\nbody\n") == (Some(Text("a: 1")), Text("body\n")),
        "a block at the top"
    );
    ensure!(
        split("---\r\na: 1\r\n---\r\nbody") == (Some(Text("a: 1")), Text("body")),
        "CRLF"
    );
    ensure!(
        split("---\na: 1\n---") == (Some(Text("a: 1")), Text("")),
        "a block that is the whole file"
    );
    ensure!(
        split("body\n---\nnot front\n---\n") == (None, Text("body\n---\nnot front\n---\n")),
        "a rule later in the text is not front matter"
    );
    ensure!(
        split("---\nnever closed\n") == (None, Text("---\nnever closed\n")),
        "an open block is text"
    );
    Ok(())
}
