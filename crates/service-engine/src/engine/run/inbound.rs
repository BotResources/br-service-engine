use std::sync::Arc;

use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobHandle;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::inbound::{DeadLetters, InboundHealth, InboundLoop, ReactionRegistry, Subscription};
use crate::nats::Nats;
use crate::offers::OfferStagers;
use crate::pipeline::{DirectPipeline, Policies};
use crate::principal::ReactionPrincipalResolver;
use crate::transport::{ImpactTransport, PgListenNotify};

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_inbound(
    pg: &PgPool,
    nats: &Nats,
    transport: &Arc<PgListenNotify>,
    accumulators: &Arc<AccumulatorRuntime>,
    offers: &Arc<OfferStagers>,
    policies: &Arc<Policies>,
    reactions: Arc<ReactionRegistry>,
    subscriptions: Vec<Subscription>,
    blob_handle: Option<BlobHandle>,
    config: &EngineConfig,
    dead_letters: &DeadLetters,
    inbound_health: &InboundHealth,
    reaction_principal: Option<Arc<dyn ReactionPrincipalResolver>>,
) -> Result<Option<InboundLoop>, EngineError> {
    if subscriptions.is_empty() {
        return Ok(None);
    }
    let pipeline = Arc::new(DirectPipeline::new(
        pg.clone(),
        transport.clone() as Arc<dyn ImpactTransport>,
        accumulators.clone(),
        offers.clone(),
        policies.clone(),
        reactions,
        blob_handle,
        config.lock_timeout,
        config.impacts_per_commit,
        config.service.clone(),
        reaction_principal,
    ));
    crate::inbound::validate_message_retention(nats, &subscriptions, config.message_retention)
        .await?;
    let loop_handle = InboundLoop::start(
        nats.clone(),
        subscriptions,
        pipeline,
        dead_letters.clone(),
        config.inbound_config(),
        inbound_health.clone(),
    )
    .await?;
    Ok(Some(loop_handle))
}
