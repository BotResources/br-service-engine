use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::accumulator::AccumulatorRuntime;
use crate::accumulator::ingress::spawn_lane_a;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::nats::Nats;

pub(crate) struct LaneATasks {
    pub stop_ingress: Arc<Notify>,
    pub stop_purge: Arc<Notify>,
    pub ingress_task: Option<JoinHandle<Result<(), EngineError>>>,
    pub purge_task: Option<JoinHandle<()>>,
}

pub(crate) fn ingress_reason(error: &EngineError) -> &'static str {
    match error {
        EngineError::SealRetentionTooShort { .. } => crate::boot::REASON_SEAL_RETENTION,
        _ => crate::boot::REASON_STREAMING_STREAM,
    }
}

pub(crate) async fn spawn_if_registered(
    nats: &Nats,
    config: &EngineConfig,
    accumulators: &Arc<AccumulatorRuntime>,
) -> Result<LaneATasks, EngineError> {
    let stop_ingress = Arc::new(Notify::new());
    let stop_purge = Arc::new(Notify::new());
    let (ingress_task, purge_task) = if accumulators.registered() > 0
        && let Some(service) = config.service.clone()
    {
        let (ingress, purge) = spawn_lane_a(
            nats,
            service,
            config.seal_retention,
            accumulators.clone(),
            config.beat,
            crate::inbound::DEFAULT_ACK_WAIT,
            crate::inbound::DEFAULT_MAX_ACK_PENDING,
            stop_ingress.clone(),
            stop_purge.clone(),
        )
        .await?;
        (Some(ingress), Some(purge))
    } else {
        (None, None)
    };
    Ok(LaneATasks {
        stop_ingress,
        stop_purge,
        ingress_task,
        purge_task,
    })
}
