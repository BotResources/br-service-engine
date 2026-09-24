use crate::engine::Engine;
use crate::error::EngineError;
use crate::principal::Principal;
use crate::stop::Stop;

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
        let server_stop = Stop::new();
        let stream_shutdown = self.stream_shutdown_sender();
        let server = tokio::spawn(crate::graphql::serve(listener, app, server_stop.clone()));
        let outcome = self.run().await;
        let _ = stream_shutdown.send(true);
        server_stop.stop();
        if let Ok(Err(error)) = server.await {
            tracing::error!(error = %crate::chain::describe(&error), "the engine's http server ended with an error");
        }
        outcome
    }
}
