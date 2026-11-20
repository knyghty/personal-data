//! Tests for the state file.

use std::env::temp_dir;
use std::fs::remove_dir_all;
use std::process;

use anyhow::{Result, ensure};
use serde_json::{Value, json};

use super::Store;
use crate::tri;

/// Values set in one store read back from a fresh one, secrets redacted, removals persisted.
#[test]
fn round_trips() -> Result<()> {
    let dir = temp_dir().join(format!("collector-store-{}", process::id()));
    let path = dir.join("state.json");
    let store = tri!(Store::try_from(path.as_path()));
    tri!(store.set("a/b", &json!({"n": 1_u32})));
    tri!(store.set("withings/tokens", &"secret"));
    let again = tri!(Store::try_from(path.as_path()));
    ensure!(
        again.get::<Value>("a/b") == Some(json!({"n": 1_u32})),
        "a set value reads back"
    );
    ensure!(
        again.redacted().get("withings/tokens") == Some(&json!("<redacted>")),
        "token values are redacted"
    );
    tri!(again.remove("a/b"));
    ensure!(
        tri!(Store::try_from(path.as_path()))
            .get::<Value>("a/b")
            .is_none(),
        "a removed key stays removed"
    );
    tri!(remove_dir_all(dir));
    Ok(())
}
