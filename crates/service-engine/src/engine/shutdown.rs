use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

use crate::engine::loops::join_presence;
use crate::error::EngineError;
use crate::housekeeping::ready::REASON_WORKER_STOPPED;
use crate::inbound::InboundLoop;
use crate::principal::Principal;
use crate::readiness::ReadinessHandle;
use crate::runtime::SessionRuntime;

pub(crate) struct StopHandles {
    pub render: Arc<Notify>,
    pub beat: Arc<Notify>,
    pub flush: Arc<Notify>,
    pub mirrors: Arc<Notify>,
    pub presence: Arc<Notify>,
    pub sched: Arc<Notify>,
    pub ingress: Arc<Notify>,
    pub purge: Arc<Notify>,
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
    stopped: Option<(&'static str, Result<(), JoinError>)>,
    stops: StopHandles,
    mut tasks: RunTasks<P>,
    readiness_guard: &ReadinessHandle,
) -> Result<(), EngineError> {
    stops.render.notify_one();
    stops.beat.notify_waiters();
    stops.flush.notify_one();
    stops.mirrors.notify_waiters();
    stops.presence.notify_waiters();
    stops.sched.notify_waiters();
    stops.ingress.notify_one();
    stops.purge.notify_one();
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
        Some((worker, result)) => {
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
