use std::sync::Arc;

use futures_util::future::BoxFuture;
use tokio::sync::Notify;

use crate::engine::Engine;
use crate::error::EngineError;
use crate::housekeeping::beat::RepairRetry;
use crate::housekeeping::gc::SessionGc;
use crate::housekeeping::ready::{REASON_WORKER_STOPPED, ReadinessAssembly};
use crate::principal::Principal;
use crate::runtime::SessionRuntime;
use crate::time::Timestamp;
use crate::transport::ImpactTransport;

enum Boot {
    Converged,
    ShuttingDown,
    MirrorStopped,
}

struct RenderGc<P: Principal>(Arc<SessionRuntime<P>>);

impl<P: Principal> SessionGc for RenderGc<P> {
    fn collect<'a>(&'a self, _now: Timestamp) -> BoxFuture<'a, Result<usize, EngineError>> {
        Box::pin(async move { Ok(self.0.gc().await) })
    }
}

struct RenderRepairs<P: Principal>(Arc<SessionRuntime<P>>);

impl<P: Principal> RepairRetry for RenderRepairs<P> {
    fn retry<'a>(&'a self) -> BoxFuture<'a, Result<usize, EngineError>> {
        Box::pin(async move { self.0.retry_repairs().await })
    }
}

impl<P: Principal> Engine<P> {
    pub async fn run(self) -> Result<(), EngineError> {
        let render = self.render();
        let Engine {
            config,
            pg,
            transport,
            readiness,
            accumulators,
            mut beat,
            mirrors,
            shutdown,
            ..
        } = self;

        let readiness_guard = readiness.clone();
        let assembly = ReadinessAssembly::new(readiness, mirrors.health())
            .with_relays(beat.relays().health())
            .with_listener(transport.listener_health());
        beat = beat
            .with_transport(transport.clone())
            .with_accumulators(accumulators.clone())
            .with_readiness(assembly)
            .with_repairs(Arc::new(RenderRepairs(render.clone())));
        beat.gc().set_sessions(Arc::new(RenderGc(render.clone())));

        let stop_render = Arc::new(Notify::new());
        let stop_beat = Arc::new(Notify::new());
        let stop_flush = Arc::new(Notify::new());
        let stop_mirrors = Arc::new(Notify::new());

        let stopping = shutdown.notified();
        tokio::pin!(stopping);
        stopping.as_mut().enable();

        let mut mirror_tasks = mirrors.start(stop_mirrors.clone());
        let boot = tokio::select! {
            converged = mirror_tasks.converged() => {
                if converged {
                    Boot::Converged
                } else {
                    Boot::MirrorStopped
                }
            }
            () = &mut stopping => Boot::ShuttingDown,
        };
        match boot {
            Boot::Converged => {}
            Boot::ShuttingDown => {
                stop_mirrors.notify_waiters();
                render.shutdown().await;
                return Ok(());
            }
            Boot::MirrorStopped => {
                stop_mirrors.notify_waiters();
                render.shutdown().await;
                readiness_guard.set_not_ready(REASON_WORKER_STOPPED);
                return Err(EngineError::WorkerStopped { worker: "mirror" });
            }
        }

        let after_pass = render.after_pass_signal();
        let render_handle = render.clone();
        let mut render_task = tokio::spawn(render.run(transport.listen(), stop_render.clone()));
        let mut beat_task = tokio::spawn(beat.run(pg.clone(), stop_beat.clone(), after_pass));
        let mut flush_task =
            tokio::spawn(accumulators.clone().run(config.window, stop_flush.clone()));

        let stopped = tokio::select! {
            () = &mut stopping => None,
            outcome = &mut render_task => Some(("render", outcome)),
            outcome = &mut beat_task => Some(("beat", outcome)),
            outcome = &mut flush_task => Some(("flush", outcome)),
            () = mirror_tasks.any_stopped() => Some(("mirror", Ok(()))),
        };

        stop_render.notify_one();
        stop_beat.notify_waiters();
        stop_flush.notify_one();
        stop_mirrors.notify_waiters();
        render_handle.shutdown().await;

        let outcome = match stopped {
            None => {
                let _ = render_task.await;
                let _ = beat_task.await;
                let _ = flush_task.await;
                Ok(())
            }
            Some((worker, result)) => {
                if let Err(join) = &result {
                    tracing::error!(worker, %join, "an engine worker panicked before shutdown");
                }
                if worker != "beat" {
                    let _ = beat_task.await;
                }
                readiness_guard.set_not_ready(REASON_WORKER_STOPPED);
                tracing::error!(
                    worker,
                    "the engine worker stopped before shutdown, so the pod is taken out of \
                     rotation and run returns loudly rather than serving readiness over a dead loop"
                );
                if worker != "render" {
                    let _ = render_task.await;
                }
                if worker != "flush" {
                    let _ = flush_task.await;
                }
                Err(EngineError::WorkerStopped { worker })
            }
        };
        drop(mirror_tasks);
        outcome
    }
}
