//! Tests against the fake GitHub API.

use anyhow::{Result, ensure};

use crate::testing;
use crate::tri;

/// A file comes back decoded; a missing one is an error.
#[tokio::test]
async fn reads_a_file_from_the_repo() -> Result<()> {
    let (api, recorded) = testing::fake_github().await;
    drop(
        testing::lock(&recorded)
            .files
            .insert("duckdb/watchlist.json".into(), "[]".into()),
    );
    let app = testing::app(&api);
    ensure!(
        tri!(app.github.file("duckdb/watchlist.json").await) == "[]",
        "the file's text"
    );
    ensure!(
        app.github.file("missing.json").await.is_err(),
        "a missing file fails"
    );
    Ok(())
}
