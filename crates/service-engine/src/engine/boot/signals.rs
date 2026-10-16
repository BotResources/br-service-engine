use std::sync::Arc;

use tokio::sync::Notify;

use crate::error::EngineError;

pub(crate) struct ProcessSignals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl ProcessSignals {
    #[cfg(unix)]
    pub(crate) fn install() -> Result<Self, EngineError> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            terminate: signal(SignalKind::terminate()).map_err(EngineError::SignalHandlers)?,
            interrupt: signal(SignalKind::interrupt()).map_err(EngineError::SignalHandlers)?,
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn install() -> Result<Self, EngineError> {
        Ok(Self {})
    }

    #[cfg(unix)]
    pub(crate) async fn received(&mut self) -> &'static str {
        tokio::select! {
            _ = self.terminate.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
        }
    }

    #[cfg(not(unix))]
    pub(crate) async fn received(&mut self) -> &'static str {
        match tokio::signal::ctrl_c().await {
            Ok(()) => "SIGINT",
            Err(error) => {
                tracing::error!(%error, "waiting for ctrl-c failed; the process handles no shutdown signal");
                std::future::pending::<&'static str>().await
            }
        }
    }

    pub(crate) fn request_shutdown_on_signal(mut self, shutdown: Arc<Notify>) {
        tokio::spawn(async move {
            let signal = self.received().await;
            tracing::info!(
                signal,
                "process signal received: starting a graceful shutdown"
            );
            shutdown.notify_one();
        });
    }
}

#[cfg(all(test, unix))]
mod tests;
