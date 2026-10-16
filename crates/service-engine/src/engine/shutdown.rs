use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};

use crate::config::EngineConfig;

use crate::engine::loops::join_presence;
use crate::error::EngineError;
use crate::housekeeping::ready::{
    REASON_SCHEMA_VERSION_DISPLACED, REASON_SHUTTING_DOWN, REASON_WORKER_STOPPED,
};
use crate::inbound::InboundLoop;
use crate::principal::Principal;
use crate::readiness::ReadinessHandle;
use crate::runtime::SessionRuntime;
use crate::stop::Stop;

pub(crate) enum Stopped {
    Worker {
        worker: &'static str,
        result: Result<(), JoinError>,
    },
    ClaimLost {
        engine_version: String,
        service_version: String,
    },
}

impl Stopped {
    pub(crate) fn worker(worker: &'static str, result: Result<(), JoinError>) -> Self {
        Self::Worker { worker, result }
    }

    pub(crate) fn claim_lost(config: &EngineConfig) -> Self {
        Self::ClaimLost {
            engine_version: crate::schema::ENGINE_SCHEMA_VERSION.to_string(),
            service_version: config.schema_service_version().to_string(),
        }
    }
}

pub(crate) async fn claim_lost(displaced: Option<watch::Receiver<bool>>) {
    if let Some(mut displaced) = displaced
        && displaced.wait_for(|lost| *lost).await.is_ok()
    {
        return;
    }
    std::future::pending::<()>().await
}

pub(crate) struct StopHandles {
    pub render: Arc<Stop>,
    pub beat: Arc<Stop>,
    pub flush: Arc<Stop>,
    pub mirrors: Arc<Stop>,
    pub presence: Arc<Stop>,
    pub sched: Arc<Stop>,
    pub ingress: Arc<Stop>,
    pub purge: Arc<Stop>,
}

pub(crate) struct RunTasks<P: Principal> {
    pub render_handle: Arc<SessionRuntime<P>>,
    pub render_task: JoinHandle<()>,
    pub beat_task: JoinHandle<()>,
    pub flush_task: JoinHandle<()>,
    pub sched_task: JoinHandle<()>,
    pub inbound: Option<InboundLoop>,
    pub ingress_task: Option<JoinHandle<()>>,
    pub purge_task: Option<JoinHandle<()>>,
    pub presence_task: Option<JoinHandle<()>>,
}

pub(crate) async fn finish<P: Principal>(
    stopped: Option<Stopped>,
    stops: StopHandles,
    mut tasks: RunTasks<P>,
    readiness_guard: &ReadinessHandle,
) -> Result<(), EngineError> {
    readiness_guard.set_not_ready(REASON_SHUTTING_DOWN);
    tasks.render_handle.begin_shutdown();
    stops.render.stop();
    stops.beat.stop();
    stops.flush.stop();
    stops.mirrors.stop();
    stops.presence.stop();
    stops.sched.stop();
    stops.ingress.stop();
    stops.purge.stop();
    if let Some(inbound) = tasks.inbound.take() {
        inbound.stop();
        inbound.join().await;
    }
    if let Some(task) = tasks.ingress_task.take() {
        let _ = task.await;
    }
    if let Some(task) = tasks.purge_task.take() {
        let _ = task.await;
    }
    let _ = tasks.sched_task.await;
    tasks.render_handle.shutdown().await;
    join_presence(tasks.presence_task.take()).await;

    match stopped {
        None => {
            let _ = tasks.render_task.await;
            let _ = tasks.beat_task.await;
            let _ = tasks.flush_task.await;
            Ok(())
        }
        Some(Stopped::ClaimLost {
            engine_version,
            service_version,
        }) => {
            let _ = tasks.beat_task.await;
            let _ = tasks.render_task.await;
            let _ = tasks.flush_task.await;
            readiness_guard.set_not_ready(REASON_SCHEMA_VERSION_DISPLACED);
            tracing::error!(
                %engine_version,
                %service_version,
                "the schema-version heartbeat updated no row: another version holds the claim, so \
                 every worker of this pod was stopped and run returns the lost claim"
            );
            Err(EngineError::SchemaClaimLost {
                engine_version,
                service_version,
            })
        }
        Some(Stopped::Worker { worker, result }) => {
            if let Err(join) = &result {
                tracing::error!(worker, %join, "an engine worker panicked before shutdown");
            }
            if worker != "beat" {
                let _ = tasks.beat_task.await;
            }
            readiness_guard.set_not_ready(REASON_WORKER_STOPPED);
            tracing::error!(
                worker,
                "the engine worker stopped before shutdown, so the pod is taken out of \
                 rotation and run returns loudly rather than serving readiness over a dead loop"
            );
            if worker != "render" {
                let _ = tasks.render_task.await;
            }
            if worker != "flush" {
                let _ = tasks.flush_task.await;
            }
            Err(EngineError::WorkerStopped { worker })
        }
    }
}
