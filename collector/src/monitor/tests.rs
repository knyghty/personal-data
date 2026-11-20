//! Tests for the Sentry cron monitor.

use anyhow::{Result, ensure};

use super::Cron;
use crate::tri;

/// The check-in URL comes from the DSN's host, project and key.
#[test]
fn derives_crons_url() -> Result<()> {
    let cron: Cron = tri!("https://abc123@o42.ingest.de.sentry.io/99".parse());
    ensure!(
        cron.url.as_deref()
            == Some("https://o42.ingest.de.sentry.io/api/99/cron/hourly-run/abc123/"),
        "check-in URL"
    );
    Ok(())
}
