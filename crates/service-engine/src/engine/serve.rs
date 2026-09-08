use std::sync::Arc;

use tokio::sync::Notify;

use crate::engine::Engine;
use crate::error::EngineError;
use crate::principal::Principal;

impl<P: Principal> Engine<P> {
    pub async fn run_with(self, app: axum::Router) -> Result<(), EngineError> {
        let addr = self.config.http_addr;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|source| EngineError::Http { addr, source })?;
        self.run_with_listener(listener, app).await
    }

    pub async fn run_with_listener(
        self,
        listener: tokio::net::TcpListener,
        app: axum::Router,
    ) -> Result<(), EngineError> {
        let server_stop = Arc::new(Notify::new());
        let server = tokio::spawn(crate::graphql::serve(listener, app, server_stop.clone()));
        let outcome = self.run().await;
        server_stop.notify_one();
        if let Ok(Err(error)) = server.await {
            tracing::error!(%error, "the engine's http server ended with an error");
        }
        outcome
    }
}
