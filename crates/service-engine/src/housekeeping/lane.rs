use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;

use crate::chain::describe;
use crate::error::EngineError;
use crate::lanes::LaneChannel;
use crate::nats::{Nats, NatsCondition, NatsHealth};

pub(crate) trait ResetAll: Send + Sync {
    fn reset_all(&self) -> BoxFuture<'_, Result<usize, EngineError>>;
}

pub(crate) struct LaneSupervisor {
    nats: Nats,
    health: NatsHealth,
    channel: LaneChannel,
    reset: Arc<dyn ResetAll>,
    paused: bool,
}

impl LaneSupervisor {
    pub(crate) fn new(
        nats: Nats,
        grace: Duration,
        channel: LaneChannel,
        reset: Arc<dyn ResetAll>,
    ) -> Self {
        Self {
            nats,
            health: NatsHealth::new(grace),
            channel,
            reset,
            paused: false,
        }
    }

    pub(crate) async fn tick(&mut self) {
        let condition = self.health.observe(self.nats.reachable(), Instant::now());
        let should_pause = matches!(condition, NatsCondition::PastGrace);
        if should_pause && !self.paused {
            self.paused = true;
            self.channel.set_paused(true);
            tracing::warn!(
                "nats has been unreachable past its grace window; the accumulated and presence \
                 lanes pause and every session that watches one is told so on its subscription",
            );
        } else if !should_pause && self.paused {
            self.paused = false;
            self.channel.set_paused(false);
            if let Err(error) = self.reset.reset_all().await {
                tracing::error!(
                    reason = %describe(&error),
                    "resetting sessions after nats returned failed; a later listener reconnect or \
                     window churn still repairs them",
                );
            }
        }
    }
}
