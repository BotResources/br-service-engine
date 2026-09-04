use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use sqlx::PgPool;
use tokio::sync::Notify;

use crate::accumulator::AccumulatorRuntime;
use crate::chain::describe;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::housekeeping::cron::{CronRound, CronRuntime};
use crate::housekeeping::gc::{Gc, GcRound};
use crate::housekeeping::ready::ReadinessAssembly;
use crate::housekeeping::relay::{RelayRound, RelayRuntime};
use crate::housekeeping::scheduled::{ScheduledBoundaries, ScheduledRound};
use crate::transport::PgListenNotify;

const MAX_BACKLOG_BURSTS: u32 = 64;

pub trait RepairRetry: Send + Sync + 'static {
    fn retry<'a>(&'a self) -> BoxFuture<'a, Result<usize, EngineError>>;
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BeatRound {
    pub relays: RelayRound,
    pub cron: CronRound,
    pub scheduled: ScheduledRound,
    pub gc: GcRound,
    pub queue_usage: Option<f64>,
    pub more: bool,
}

pub struct Beat {
    interval: Duration,
    relays: RelayRuntime,
    cron: CronRuntime,
    gc: Gc,
    scheduled: Option<ScheduledBoundaries>,
    transport: Option<Arc<PgListenNotify>>,
    readiness: Option<ReadinessAssembly>,
    repairs: Option<Arc<dyn RepairRetry>>,
}

impl Beat {
    pub fn from_config(config: &EngineConfig) -> Result<Self, EngineError> {
        config.validate()?;
        Ok(Self {
            interval: config.beat,
            relays: RelayRuntime::new(config.pod_id.clone())
                .with_lease(config.lease)
                .with_slot_period(config.beat)
                .with_batch(crate::housekeeping::relay::DEFAULT_BATCH),
            cron: CronRuntime::from_config(config),
            gc: Gc::new(),
            scheduled: None,
            transport: None,
            readiness: None,
            repairs: None,
        })
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

    pub fn with_readiness(mut self, readiness: ReadinessAssembly) -> Self {
        self.readiness = Some(readiness);
        self
    }

    pub fn with_repairs(mut self, repairs: Arc<dyn RepairRetry>) -> Self {
        self.repairs = Some(repairs);
        self
    }

    pub fn relays(&mut self) -> &mut RelayRuntime {
        &mut self.relays
    }

    pub fn cron(&mut self) -> &mut CronRuntime {
        &mut self.cron
    }

    pub fn gc(&mut self) -> &mut Gc {
        &mut self.gc
    }

    pub fn readiness(&self) -> Option<&ReadinessAssembly> {
        self.readiness.as_ref()
    }

    pub const fn interval(&self) -> Duration {
        self.interval
    }

    pub const fn slot_retention(&self) -> Duration {
        self.cron.slot_retention()
    }

    pub async fn tick(&mut self, pg: &PgPool) -> BeatRound {
        if let Some(repairs) = &self.repairs
            && let Err(error) = repairs.retry().await
        {
            tracing::warn!(
                reason = %describe(&error),
                "the beat could not retry a pending session repair",
            );
        }
        let relays = self.relays.beat(pg).await;
        let cron = self.cron.beat(pg).await;
        let scheduled = self.fire_boundaries().await;
        let gc = self.gc.sweep(pg, self.cron.slot_retention()).await;
        if let Some(readiness) = &self.readiness {
            readiness.refresh();
        }
        let round = BeatRound {
            more: relays.more || scheduled.more,
            queue_usage: self.queue_usage().await,
            relays,
            cron,
            scheduled,
            gc,
        };
        crate::observe::record_beat(&round);
        round
    }

    async fn fire_boundaries(&self) -> ScheduledRound {
        match &self.scheduled {
            None => ScheduledRound::default(),
            Some(boundaries) => match boundaries.fire_due().await {
                Ok(round) => round,
                Err(error) => {
                    tracing::warn!(
                        reason = %describe(&error),
                        "the beat could not claim the boundaries whose time has passed",
                    );
                    ScheduledRound::default()
                }
            },
        }
    }

    async fn drain_backlog(&mut self, pg: &PgPool) -> bool {
        let relays = self.relays.beat(pg).await;
        let scheduled = self.fire_boundaries().await;
        relays.more || scheduled.more
    }

    pub async fn run(mut self, pg: PgPool, shutdown: Arc<Notify>, after_pass: Arc<Notify>) {
        let stopping = shutdown.notified();
        tokio::pin!(stopping);
        stopping.as_mut().enable();
        let mut next_tick = Instant::now();
        loop {
            if Instant::now() >= next_tick {
                let mut more = self.tick(&pg).await.more;
                let mut bursts = 0;
                while more && bursts < MAX_BACKLOG_BURSTS {
                    bursts += 1;
                    more = tokio::select! {
                        biased;
                        () = &mut stopping => return,
                        more = self.drain_backlog(&pg) => more,
                    };
                    tokio::task::yield_now().await;
                }
                next_tick = Instant::now() + if more { Duration::ZERO } else { self.interval };
            }
            let pause = next_tick.saturating_duration_since(Instant::now());
            tokio::select! {
                biased;
                () = &mut stopping => return,
                () = after_pass.notified() => {
                    self.relays.after_pass(&pg).await;
                }
                () = tokio::time::sleep(pause) => {}
            }
        }
    }

    async fn queue_usage(&self) -> Option<f64> {
        let transport = self.transport.as_ref()?;
        match transport.queue_usage().await {
            Ok(usage) => Some(usage),
            Err(error) => {
                tracing::warn!(
                    reason = %describe(&error),
                    "the notification queue usage could not be sampled",
                );
                None
            }
        }
    }
}

impl std::fmt::Debug for Beat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Beat")
            .field("interval", &self.interval)
            .field("relays", &self.relays.names())
            .field("cron", &self.cron.names())
            .field("scheduled", &self.scheduled.is_some())
            .field("readiness", &self.readiness.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name::{ChannelName, PodId};

    fn config() -> EngineConfig {
        EngineConfig::new(
            ChannelName::from_static("service_engine_impact"),
            PodId::from_static("svc-sample-0"),
        )
    }

    #[test]
    fn a_beat_takes_its_period_lease_and_retention_from_the_one_engine_config() {
        let beat = Beat::from_config(&config().with_beat(Duration::from_millis(250))).unwrap();
        assert_eq!(beat.interval(), Duration::from_millis(250));
        assert!(beat.readiness().is_none());
    }

    #[test]
    fn a_config_that_would_expire_a_lease_before_its_holder_renews_it_never_starts_a_beat() {
        assert!(Beat::from_config(&config().with_lease(Duration::from_secs(1))).is_err());
    }

    #[test]
    fn a_scheduled_batch_without_a_transport_to_claim_through_is_refused() {
        let beat = Beat::from_config(&config()).unwrap();
        assert!(beat.with_scheduled_batch(16).is_err());
    }

    #[test]
    fn one_retention_governs_both_the_catch_up_horizon_and_the_slot_collector() {
        let mut beat = Beat::from_config(&config())
            .unwrap()
            .with_slot_retention(Duration::from_secs(3600));
        assert_eq!(beat.slot_retention(), Duration::from_secs(3600));
        beat.cron().set_slot_retention(Duration::from_secs(60));
        assert_eq!(
            beat.slot_retention(),
            Duration::from_secs(60),
            "the collector is handed the horizon the cron layer holds, so there is nothing to \
             desynchronise: collecting a slot row the cron layer would still claim makes the job \
             run twice"
        );
    }

    #[test]
    fn a_round_that_left_work_behind_asks_the_beat_to_come_back_before_it_sleeps() {
        let round = BeatRound {
            relays: RelayRound {
                more: true,
                ..RelayRound::default()
            },
            ..BeatRound::default()
        };
        assert!(round.relays.more);
        assert!(!BeatRound::default().more);
    }
}
