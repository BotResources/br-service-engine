use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio::sync::Notify;

use crate::blobs::BoundBlobs;
use crate::engine::Engine;
use crate::engine::loops::{RenderGc, RenderRepairs, join_presence, run_scheduled_messages};
use crate::error::{EngineError, TransportError};
use crate::housekeeping::ready::{REASON_WORKER_STOPPED, ReadinessAssembly};
use crate::impact::TransportEvent;
use crate::inbound::{DeadLetters, InboundConfig, InboundLoop};
use crate::pipeline::DirectPipeline;
use crate::presence::REASON_PRESENCE_BUCKET;
use crate::principal::Principal;
use crate::transport::ImpactTransport;

enum Boot {
    Converged,
    ShuttingDown,
    MirrorStopped,
}

impl<P: Principal> Engine<P> {
    pub async fn run(self) -> Result<(), EngineError> {
        let slices = crate::graphql::SchemaSlices::assemble(&self.schema_slices)?;
        if let Some(sdl) = &self.schema_sdl {
            slices.verify_root_fields(sdl)?;
        }
        let render = self.render_runtime();
        let erasure_drain: Option<Arc<dyn crate::erase::ErasureDrain>> = if self.erasables.is_empty()
        {
            None
        } else {
            Some(Arc::new(self.eraser()))
        };
        let Engine {
            config,
            pg,
            nats,
            transport,
            readiness,
            accumulators,
            mut beat,
            mirrors,
            inbound_reactions,
            offers,
            presence,
            blobs,
            shutdown,
            declared_scopes,
            ..
        } = self;
        let offers = Arc::new(offers);

        beat.relays().register_erased(Arc::new(
            crate::relays::outbox::HostedOutboxRelay::hosting(
                crate::name::RelayName::from_static("integration_outbox"),
                crate::relays::outbox::OutboxRelay::new(pg.clone(), nats.clone()),
                config.impacts_per_commit.max(1),
            ),
        ))?;

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
        if let Some(drain) = erasure_drain {
            beat = beat.with_erasure_drain(drain);
        }
        beat.gc().set_sessions(Arc::new(RenderGc(render.clone())));

        let stop_render = Arc::new(Notify::new());
        let stop_beat = Arc::new(Notify::new());
        let stop_flush = Arc::new(Notify::new());
        let stop_mirrors = Arc::new(Notify::new());
        let stop_presence = Arc::new(Notify::new());
        let stop_sched = Arc::new(Notify::new());

        let stopping = shutdown.notified();
        tokio::pin!(stopping);
        stopping.as_mut().enable();

        let mut mirror_tasks = mirrors.start(stop_mirrors.clone());
        let boot = tokio::select! {
            converged = mirror_tasks.converged() => {
                if converged { Boot::Converged } else { Boot::MirrorStopped }
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

        let blob_handle = match crate::blobs::bind(&blobs, &config).await {
            Ok(BoundBlobs { handle, reaper }) => {
                if let Some(reaper) = reaper {
                    beat = beat.with_blob_reaper(reaper);
                }
                handle
            }
            Err((error, reason)) => {
                readiness_guard.set_not_ready(reason);
                stop_mirrors.notify_waiters();
                stop_presence.notify_waiters();
                join_presence(presence_task.take()).await;
                render.shutdown().await;
                return Err(error);
            }
        };

        if let Some(declaration) = declared_scopes {
            readiness_guard.set_not_ready(crate::scopes::REASON_SCOPES_PENDING);
            let handshake = crate::scopes::run_handshake(&nats, declaration);
            tokio::pin!(handshake);
            let outcome = tokio::select! {
                () = &mut stopping => {
                    stop_mirrors.notify_waiters();
                    stop_presence.notify_waiters();
                    join_presence(presence_task.take()).await;
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
                stop_presence.notify_waiters();
                join_presence(presence_task.take()).await;
                render.shutdown().await;
                return Err(EngineError::Scope(error));
            }
        }

        let reactions = Arc::new(
            inbound_reactions
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let subscriptions = reactions.subscriptions();
        let inbound = if subscriptions.is_empty() {
            None
        } else {
            let pipeline = Arc::new(DirectPipeline::new(
                pg.clone(),
                transport.clone() as Arc<dyn ImpactTransport>,
                accumulators.clone(),
                offers.clone(),
                reactions.clone(),
                blob_handle.clone(),
                config.lock_timeout,
                config.impacts_per_commit,
            ));
            match InboundLoop::start(
                nats.clone(),
                subscriptions,
                pipeline,
                DeadLetters::new(pg.clone())
                    .with_transport(transport.clone() as Arc<dyn ImpactTransport>),
                InboundConfig::default(),
            )
            .await
            {
                Ok(loop_handle) => Some(loop_handle),
                Err(error) => {
                    readiness_guard.set_not_ready(crate::nats::REASON_NO_STREAM);
                    stop_mirrors.notify_waiters();
                    stop_presence.notify_waiters();
                    join_presence(presence_task.take()).await;
                    render.shutdown().await;
                    return Err(error);
                }
            }
        };

        let sched_task = tokio::spawn(run_scheduled_messages(
            pg.clone(),
            nats.clone(),
            config.beat,
            stop_sched.clone(),
        ));

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
        stop_sched.notify_waiters();
        if let Some(inbound) = inbound {
            inbound.stop();
            inbound.join().await;
        }
        let _ = sched_task.await;
        render_handle.shutdown().await;
        join_presence(presence_task.take()).await;

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
