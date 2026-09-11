use std::sync::Arc;
use std::time::{Duration, Instant};

use service_engine::config::EngineConfig;
use service_engine::erase::{EraseOutcome, Eraser, PersonId};
use service_engine::error::EngineError;
use service_engine::nats::Nats;
use service_engine::{AccumulatorRuntime, Engine, Settle};
use service_engine::{Readiness, ReadinessHandle};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::kernel::AppPrincipal;

pub struct Service {
    pub base_url: String,
    pub chunks: Arc<AccumulatorRuntime>,
    settle: Settle<AppPrincipal>,
    eraser: Eraser<AppPrincipal>,
    readiness: ReadinessHandle,
    stop: Arc<Notify>,
    handle: JoinHandle<Result<(), EngineError>>,
}

impl Service {
    pub fn http(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub fn ws(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.replacen("http://", "ws://", 1))
    }

    pub fn readiness(&self) -> &ReadinessHandle {
        &self.readiness
    }

    pub async fn settle(&self) {
        self.settle
            .settle(Duration::from_millis(150), Duration::from_secs(5))
            .await;
    }

    pub async fn erase(&self, person: PersonId) -> Result<EraseOutcome, EngineError> {
        self.eraser.erase(person).await
    }

    pub async fn shutdown(self) {
        self.stop.notify_one();
        let _ = self.handle.await;
    }

    pub async fn run(self) -> Result<(), EngineError> {
        match self.handle.await {
            Ok(result) => result,
            Err(join) => Err(EngineError::Config(format!("engine task panicked: {join}"))),
        }
    }
}

pub struct BootOptions {
    pub declare_scopes: bool,
    pub await_ready: bool,
}

impl Default for BootOptions {
    fn default() -> Self {
        Self {
            declare_scopes: true,
            await_ready: true,
        }
    }
}

pub async fn assemble(
    config: EngineConfig,
    pool: PgPool,
    nats: Nats,
    readiness: ReadinessHandle,
    options: &BootOptions,
) -> Result<(Engine<AppPrincipal>, axum::Router), EngineError> {
    let mut engine = Engine::<AppPrincipal>::boot(config, pool, nats, readiness.clone()).await?;
    crate::register::all(&mut engine)?;
    if options.declare_scopes {
        engine.declare_contributed_scopes()?;
    }
    let app = crate::graphql::build(&mut engine, readiness);
    Ok((engine, app))
}

pub async fn boot(
    config: EngineConfig,
    pool: PgPool,
    nats: Nats,
    options: BootOptions,
) -> Result<Service, EngineError> {
    let http_addr = config.http_addr;
    let readiness = ReadinessHandle::not_ready("booting");
    let (engine, app) = assemble(config, pool, nats, readiness.clone(), &options).await?;
    let stop = engine.shutdown_handle();
    let chunks = engine.accumulator_handle();
    let settle = engine.settle_handle();
    let eraser = engine.eraser();

    let listener = TcpListener::bind(http_addr)
        .await
        .map_err(|source| EngineError::Http {
            addr: http_addr,
            source,
        })?;
    let addr = listener.local_addr().map_err(|source| EngineError::Http {
        addr: http_addr,
        source,
    })?;
    let handle = tokio::spawn(engine.run_with_listener(listener, app));

    if options.await_ready {
        let deadline = Instant::now() + Duration::from_secs(30);
        while readiness.snapshot() != Readiness::Ready {
            if Instant::now() >= deadline {
                let reason = format!("{:?}", readiness.snapshot());
                stop.notify_one();
                return Err(EngineError::Config(format!(
                    "the engine never reached readiness UP within the boot deadline; last state: {reason}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    Ok(Service {
        base_url: format!("http://{addr}"),
        chunks,
        settle,
        eraser,
        readiness,
        stop,
        handle,
    })
}
