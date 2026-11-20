//! Collect raw personal data from several sources and commit it to a GitHub repo.

extern crate alloc;

pub mod app;
pub mod github;
pub mod monitor;
pub mod raw;
pub mod sources;
pub mod storage;
pub mod store;
#[cfg(test)]
pub mod testing;
#[cfg(test)]
mod tests;
mod tri;

use alloc::sync::Arc;
use std::env;

use anyhow::Result;
use futures_util::future::select;
use sentry::ClientOptions;
use sentry::integrations::tracing::layer as sentry_layer;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::signal::ctrl_c;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _, fmt};

use crate::app::Env;

fn main() -> Result<()> {
    // Sentry must start before the async runtime. Keep the guard alive until exit so queued events are flushed.
    let mut options = ClientOptions::new().maybe_release(sentry::release_name!());
    options.dsn = env::var("SENTRY_DSN").ok().and_then(|dsn| dsn.parse().ok());
    let _sentry = sentry::init(options);
    tracing_subscriber::registry()
        .with(fmt::layer().with_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        ))
        .with(sentry_layer())
        .init();
    let runtime = tri!(Builder::new_multi_thread().enable_all().build());
    let served = runtime.block_on(async {
        let app = Arc::new(tri!(Env::from(env::vars()).app()));
        let listener = tri!(TcpListener::bind("0.0.0.0:8080").await);
        let mut terminate = tri!(signal(SignalKind::terminate()));
        info!("listening on 8080");
        tri!(
            axum::serve(listener, Arc::clone(&app).router())
                .with_graceful_shutdown(async move {
                    // Either signal ends the wait; which one does not matter.
                    drop(select(Box::pin(ctrl_c()), Box::pin(terminate.recv())).await);
                    info!("shutdown requested, finishing in-flight work");
                })
                .await
        );
        let _was_open = app.tasks.close();
        app.tasks.wait().await;
        info!("shut down");
        Ok(())
    });
    drop(runtime);
    served
}
