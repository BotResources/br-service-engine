use std::sync::Arc;
use std::time::Duration;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobReaper;
use crate::erase::ErasureDrain;
use crate::error::EngineError;
use crate::housekeeping::beat::{Beat, RepairRetry};
use crate::housekeeping::lane::LaneSupervisor;
use crate::housekeeping::ready::ReadinessAssembly;
use crate::housekeeping::scheduled::ScheduledBoundaries;
use crate::transport::PgListenNotify;

impl Beat {
    pub(crate) fn with_lane_supervisor(mut self, lane: LaneSupervisor) -> Self {
        self.lane = Some(lane);
        self
    }

    pub(crate) fn with_blob_reaper(mut self, reaper: BlobReaper) -> Self {
        self.blob_reaper = Some(reaper);
        self
    }

    pub(crate) fn with_erasure_drain(mut self, drain: Arc<dyn ErasureDrain>) -> Self {
        self.erasures = Some(drain);
        self
    }

    pub fn with_transport(mut self, transport: Arc<PgListenNotify>) -> Self {
        self.scheduled = Some(ScheduledBoundaries::new(transport.clone()));
        self.transport = Some(transport);
        self
    }

    pub fn with_scheduled_batch(mut self, batch: i64) -> Result<Self, EngineError> {
        self.scheduled = match self.scheduled.take() {
            Some(scheduled) => Some(scheduled.with_batch(batch)?),
            None => {
                return Err(EngineError::Config(
                    "a scheduled-boundary batch needs a transport to claim through".into(),
                ));
            }
        };
        Ok(self)
    }

    pub fn with_accumulators(mut self, accumulators: Arc<AccumulatorRuntime>) -> Self {
        self.gc = self.gc.with_accumulators(accumulators);
        self
    }

    pub fn with_slot_retention(mut self, retention: Duration) -> Self {
        self.cron.set_slot_retention(retention);
        self
    }

    pub fn with_gc_interval(mut self, interval: Duration) -> Self {
        self.gc.set_interval(interval);
        self
    }

    pub fn with_dead_letters(mut self, dead_letters: crate::inbound::DeadLetters) -> Self {
        self.cron.set_dead_letters(dead_letters);
        self
    }

    pub fn with_readiness(mut self, readiness: ReadinessAssembly) -> Self {
        self.readiness = Some(readiness);
        self
    }

    pub fn with_repairs(mut self, repairs: Arc<dyn RepairRetry>) -> Self {
        self.repairs = Some(repairs);
        self
    }
}
