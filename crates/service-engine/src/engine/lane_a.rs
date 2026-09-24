use crate::error::AccumulatorError;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::accumulator::AccumulatorRuntime;
use crate::accumulator::lane_a_loop::spawn_lane_a;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::nats::Nats;
use crate::stop::Stop;

pub(crate) struct LaneATasks {
    pub stop_ingress: Arc<Stop>,
    pub stop_purge: Arc<Stop>,
    pub ingress_task: Option<JoinHandle<()>>,
    pub purge_task: Option<JoinHandle<()>>,
}

pub(crate) fn ingress_reason(error: &EngineError) -> &'static str {
    match error {
        EngineError::Accumulator(AccumulatorError::SealRetentionTooShort { .. }) => {
            crate::boot::REASON_SEAL_RETENTION
        }
        EngineError::Accumulator(AccumulatorError::AccumulatorWithoutService) => {
            crate::boot::REASON_ACCUMULATOR_NO_SERVICE
        }
        _ => crate::boot::REASON_STREAMING_STREAM,
    }
}

pub(crate) async fn spawn_if_registered(
    nats: &Nats,
    config: &EngineConfig,
    accumulators: &Arc<AccumulatorRuntime>,
    health: crate::inbound::InboundHealth,
) -> Result<LaneATasks, EngineError> {
    let stop_ingress = Stop::new();
    let stop_purge = Stop::new();
    let (ingress_task, purge_task) = if accumulators.registered() > 0 {
        let Some(service) = config.service.clone() else {
            return Err(EngineError::Accumulator(
                AccumulatorError::AccumulatorWithoutService,
            ));
        };
        accumulators.bind_lane_a_purge(nats.clone(), service.clone());
        let (ingress, purge) = spawn_lane_a(
            nats,
            service,
            config.seal_retention,
            accumulators.clone(),
            config.beat,
            config.ack_wait,
            config.max_ack_pending,
            health,
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
