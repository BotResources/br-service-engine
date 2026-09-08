use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use tokio::sync::Notify;

use crate::engine::Engine;
use crate::error::{EngineError, TransportError};
use crate::housekeeping::beat::RepairRetry;
use crate::housekeeping::gc::SessionGc;
use crate::housekeeping::ready::{REASON_WORKER_STOPPED, ReadinessAssembly};
use crate::impact::TransportEvent;
use crate::presence::REASON_PRESENCE_BUCKET;
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
        let render = self.render_runtime();
        let Engine {
            config,
            pg,
            nats,
            transport,
            readiness,
            accumulators,
            mut beat,
            mirrors,
            presence,
            shutdown,
            declared_scopes,
            ..
        } = self;

        let readiness_guard = readiness.clone();
        let assembly = ReadinessAssembly::new(readiness, mirrors.health())
            .with_relays(beat.relays().health())
            .with_listener(transport.listener_health())
            .with_nats(nats.clone(), config.nats_grace);
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

        let stop_presence = Arc::new(Notify::new());
        let mut presence_task = None;
        let events: BoxStream<'static, Result<TransportEvent, TransportError>> = if presence
            .is_empty()
        {
            transport.listen()
        } else {
            let Some(bucket_name) = config.ephemeral_bucket() else {
                readiness_guard.set_not_ready(REASON_PRESENCE_BUCKET);
                render.shutdown().await;
                return Err(EngineError::Config(
                    "a presence lane is registered but no service is configured, so the \
                     EPHEMERAL_{service} bucket has no name; call EngineConfig::with_service"
                        .into(),
                ));
            };
            let lanes = presence.lanes();
            let bucket = match crate::presence::bind_and_seed(&nats, &bucket_name, &lanes).await {
                Ok(bucket) => bucket,
                Err(error) => {
                    readiness_guard.set_not_ready(REASON_PRESENCE_BUCKET);
                    render.shutdown().await;
                    return Err(error);
                }
            };
            let _ = presence.bucket_slot().set(bucket.clone());
            let (sender, stream) = crate::presence::presence_channel();
            presence_task = Some(tokio::spawn(crate::presence::run_watch(
                bucket,
                lanes,
                sender,
                stop_presence.clone(),
            )));
            futures_util::stream::select(transport.listen(), stream).boxed()
        };
        if let Some(declaration) = declared_scopes {
            readiness_guard.set_not_ready(crate::scopes::REASON_SCOPES_PENDING);
            let handshake = crate::scopes::run_handshake(&nats, declaration);
            tokio::pin!(handshake);
            let outcome = tokio::select! {
                () = &mut stopping => {
                    stop_mirrors.notify_waiters();
                    render.shutdown().await;
                    return Ok(());
                }
                outcome = &mut handshake => outcome,
            };
            if let Err(error) = outcome {
                readiness_guard.set_not_ready(error.readiness_reason());
                tracing::error!(
                    %error,
                    "scope declaration did not complete; the pod stays out of rotation rather \
                     than serving with unconfirmed scopes"
                );
                stop_mirrors.notify_waiters();
                render.shutdown().await;
                return Err(EngineError::Scope(error));
            }
        }

        let after_pass = render.after_pass_signal();
        let render_handle = render.clone();
        let mut render_task = tokio::spawn(render.run(events, stop_render.clone()));
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
        stop_presence.notify_waiters();
        render_handle.shutdown().await;
        if let Some(task) = presence_task.take() {
            let _ = task.await;
        }

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
