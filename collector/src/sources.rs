//! One module per data source. Add a source by implementing `Source` and listing it in `all()`.

pub(super) mod albert_heijn;
pub(super) mod apple_health;
pub(super) mod audible;
pub(super) mod bunq;
pub(super) mod calendar;
// Exported, so that `Login` can be `pub(crate)` for the re-export below.
pub mod degiro;
pub(super) mod earthquakes;
pub(super) mod github_activity;
pub(super) mod gmail;
pub(super) mod google;
pub(super) mod home_assistant;
pub(super) mod kindle;
pub(super) mod knmi;
pub(super) mod knmi_edr;
pub(super) mod knmi_radar;
pub(super) mod lastfm;
pub(super) mod luchtmeetnet;
pub(super) mod moneybird;
pub(super) mod network;
pub(super) mod offers;
pub(super) mod screen_time;
pub(super) mod steam;
pub(super) mod twitch;
pub(super) mod water;
pub(super) mod weather;
pub(super) mod withings;
pub(super) mod youtube;

use alloc::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;

pub(super) use degiro::Login as DegiroLogin;

use crate::app::App;

/// A data source: a name under `raw/`, optional routes and scheduled jobs.
#[async_trait]
pub(crate) trait Source: Send + Sync {
    /// Work to do about once a day.
    async fn daily(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    /// Work to do on every scheduled run.
    async fn hourly(&self, _app: &App) -> Result<()> {
        Ok(())
    }

    /// Directory name under `raw/`.
    fn name(&self) -> &'static str;

    /// HTTP routes this source exposes, mounted at the root.
    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// A source that queues lookups from the browser visits in a screen time push.
pub(super) trait Watches {
    /// Queue what `push` mentions that was not looked up or queued yet.
    fn queue(&self, app: &App, push: &str);
}

/// Every source, in the order their jobs are reported.
pub(crate) fn all() -> Vec<Box<dyn Source>> {
    vec![
        Box::new(withings::Withings),
        Box::new(apple_health::AppleHealth),
        Box::new(screen_time::ScreenTime),
        Box::new(network::Network),
        Box::new(home_assistant::HomeAssistant),
        Box::new(weather::Weather),
        Box::new(luchtmeetnet::Luchtmeetnet),
        Box::new(bunq::PERSONAL),
        Box::new(bunq::BUSINESS),
        Box::new(steam::Steam),
        Box::new(lastfm::Lastfm),
        Box::new(github_activity::GitHubActivity),
        Box::new(calendar::Calendar),
        Box::new(albert_heijn::AlbertHeijn),
        Box::new(youtube::YouTube),
        Box::new(google::Google),
        Box::new(twitch::Twitch),
        Box::new(audible::Audible),
        Box::new(kindle::Kindle),
        Box::new(water::Water),
        Box::new(knmi::Knmi),
        Box::new(knmi_edr::KnmiEdr),
        Box::new(knmi_radar::KnmiRadar),
        Box::new(earthquakes::Earthquakes),
        Box::new(gmail::Gmail),
        Box::new(moneybird::Moneybird),
        Box::new(degiro::PERSONAL),
        Box::new(degiro::PENSION),
        Box::new(offers::Offers),
    ]
}
