use std::sync::Arc;

use sqlx::PgPool;

use crate::accumulator::runtime::AccumulatorRuntime;
use crate::config::EngineConfig;
use crate::engine::loops::{RenderGc, RenderRepairs, RenderReset};
use crate::error::EngineError;
use crate::housekeeping::beat::Beat;
use crate::housekeeping::mirror::MirrorSupervisor;
use crate::housekeeping::ready::ReadinessAssembly;
use crate::inbound::{DeadLetters, InboundHealth};
use crate::nats::Nats;
use crate::principal::Principal;
use crate::readiness::ReadinessHandle;
use crate::runtime::SessionRuntime;
use crate::transport::{ImpactTransport, PgListenNotify};

#[allow(clippy::too_many_arguments)]
pub(super) fn wire_beat<P: Principal>(
    mut beat: Beat,
    pg: &PgPool,
    nats: &Nats,
    config: &EngineConfig,
    transport: &Arc<PgListenNotify>,
    readiness: ReadinessHandle,
    mirrors: &mut MirrorSupervisor,
    accumulators: &Arc<AccumulatorRuntime>,
    render: &Arc<SessionRuntime<P>>,
    erasure_drain: Option<Arc<dyn crate::erase::ErasureDrain>>,
) -> Result<(Beat, DeadLetters, InboundHealth), EngineError> {
    let dead_letters =
        DeadLetters::new(pg.clone()).with_transport(transport.clone() as Arc<dyn ImpactTransport>);
    mirrors.set_dead_letters(dead_letters.clone());
    // The render pass records poison documents (a projection that returned Err)
    // through the same transport-backed store, so a render dead-letter raises
    // the ops-view impact exactly as the message sources do.
    render.set_dead_letters(dead_letters.clone());
    let (inbound_health, inbound_health_rx) = InboundHealth::new();

    beat.relays().register_erased(Arc::new(
        crate::relays::outbox::HostedOutboxRelay::hosting(
            crate::name::RelayName::from_static("integration_outbox"),
            crate::relays::outbox::OutboxRelay::new(pg.clone(), nats.clone())
                .with_dead_letters(dead_letters.clone()),
        )
        .with_message_retention(config.message_retention),
    ))?;

    let (schema_displaced_tx, schema_displaced_rx) = tokio::sync::watch::channel(false);
    let assembly = ReadinessAssembly::new(readiness, mirrors.health())
        .with_relays(beat.relays().health())
        .with_listener(transport.listener_health())
        .with_inbound_health(inbound_health_rx)
        .with_nats(nats.clone(), config.nats_grace)
        .with_schema_version(schema_displaced_rx);
    beat = beat
        .with_transport(transport.clone())
        .with_accumulators(accumulators.clone())
        .with_dead_letters(dead_letters.clone())
        .with_readiness(assembly)
        .with_schema_displaced(schema_displaced_tx)
        .with_repairs(Arc::new(RenderRepairs(render.clone())));
    if let Some(drain) = erasure_drain {
        beat = beat.with_erasure_drain(drain);
    }
    if render.lane_channel().has_session_lanes() {
        let reset: Arc<dyn crate::housekeeping::lane::ResetAll> =
            Arc::new(RenderReset(render.clone()));
        beat = beat.with_lane_supervisor(crate::housekeeping::lane::LaneSupervisor::new(
            nats.clone(),
            config.nats_grace,
            render.lane_channel(),
            reset,
        ));
    }
    beat.gc().set_sessions(Arc::new(RenderGc(render.clone())));
    Ok((beat, dead_letters, inbound_health))
}
