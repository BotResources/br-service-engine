use std::sync::Arc;

use tokio::sync::Notify;

use crate::blobs::BoundBlobs;
use crate::engine::Engine;
use crate::engine::loops::{join_presence, run_scheduled_messages};
use crate::error::EngineError;
use crate::housekeeping::ready::REASON_WORKER_STOPPED;
use crate::inbound::InboundLoop;
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
        let erasure_drain: Option<Arc<dyn crate::erase::ErasureDrain>> =
            if self.erasables.is_empty() {
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
            beat,
            mut mirrors,
            inbound_reactions,
            offers,
            presence,
            blobs,
            shutdown,
            declared_scopes,
            reaction_principal,
            ..
        } = self;
        let offers = Arc::new(offers);
        let readiness_guard = readiness.clone();
        let (mut beat, dead_letters, inbound_health) = crate::engine::wiring::wire_beat(
            beat,
            &pg,
            &nats,
            &config,
            &transport,
            readiness,
            &mut mirrors,
            &accumulators,
            &render,
            erasure_drain,
        )?;

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

        let (events, mut presence_task) = match crate::engine::loops::open_presence_stream(
            &nats,
            &config,
            &presence,
            &transport,
            stop_presence.clone(),
        )
        .await
        {
            Ok(pair) => pair,
            Err(error) => {
                readiness_guard.set_not_ready(REASON_PRESENCE_BUCKET);
                render.shutdown().await;
                return Err(error);
            }
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
        let mut inbound = if subscriptions.is_empty() {
            None
        } else {
            if let Err(error) = crate::inbound::validate_message_retention(
                &nats,
                &subscriptions,
                config.message_retention,
            )
            .await
            {
                readiness_guard.set_not_ready(crate::nats::REASON_NO_STREAM);
                stop_mirrors.notify_waiters();
                stop_presence.notify_waiters();
                join_presence(presence_task.take()).await;
                render.shutdown().await;
                return Err(error);
            }
            let pipeline = Arc::new(DirectPipeline::new(
                pg.clone(),
                transport.clone() as Arc<dyn ImpactTransport>,
                accumulators.clone(),
                offers.clone(),
                reactions.clone(),
                blob_handle.clone(),
                config.lock_timeout,
                config.impacts_per_commit,
                config.service.clone(),
                reaction_principal.clone(),
            ));
            match InboundLoop::start(
                nats.clone(),
                subscriptions,
                pipeline,
                dead_letters.clone(),
                config.inbound_config(),
                inbound_health.clone(),
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

        let lane_a = match crate::engine::lane_a::spawn_if_registered(
            &nats,
            &config,
            &accumulators,
            inbound_health.clone(),
        )
        .await
        {
            Ok(tasks) => tasks,
            Err(error) => {
                readiness_guard.set_not_ready(crate::engine::lane_a::ingress_reason(&error));
                stop_mirrors.notify_waiters();
                stop_presence.notify_waiters();
                join_presence(presence_task.take()).await;
                if let Some(inbound) = inbound.take() {
                    inbound.stop();
                    inbound.join().await;
                }
                render.shutdown().await;
                return Err(error);
            }
        };
        let crate::engine::lane_a::LaneATasks {
            stop_ingress,
            stop_purge,
            mut ingress_task,
            mut purge_task,
        } = lane_a;

        let sched_task = tokio::spawn(run_scheduled_messages(
            pg.clone(),
            nats.clone(),
            dead_letters.clone(),
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

        let outcome = crate::engine::shutdown::finish(
            stopped,
            crate::engine::shutdown::StopHandles {
                render: stop_render,
                beat: stop_beat,
                flush: stop_flush,
                mirrors: stop_mirrors,
                presence: stop_presence,
                sched: stop_sched,
                ingress: stop_ingress,
                purge: stop_purge,
            },
            crate::engine::shutdown::RunTasks {
                render_handle,
                render_task,
                beat_task,
                flush_task,
                sched_task,
                inbound: inbound.take(),
                ingress_task: ingress_task.take(),
                purge_task: purge_task.take(),
                presence_task: presence_task.take(),
            },
            &readiness_guard,
        )
        .await;
        drop(mirror_tasks);
        outcome
    }
}
