use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use sqlx::PgPool;
use tokio::sync::Notify;

use crate::blobs::{BlobReaper, ReaperRound};
use crate::chain::describe;
use crate::config::EngineConfig;
use crate::erase::ErasureDrain;
use crate::error::EngineError;
use crate::housekeeping::cron::{CronRound, CronRuntime};
use crate::housekeeping::gc::{Gc, GcRound};
use crate::housekeeping::lane::LaneSupervisor;
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
    pub blobs: ReaperRound,
    pub queue_usage: Option<f64>,
    pub more: bool,
}

pub struct Beat {
    interval: Duration,
    pub(super) relays: RelayRuntime,
    pub(super) cron: CronRuntime,
    pub(super) gc: Gc,
    pub(super) scheduled: Option<ScheduledBoundaries>,
    pub(super) transport: Option<Arc<PgListenNotify>>,
    pub(super) readiness: Option<ReadinessAssembly>,
    pub(super) repairs: Option<Arc<dyn RepairRetry>>,
    pub(super) blob_reaper: Option<BlobReaper>,
    listener_queue_threshold: f64,
    schema_version: (String, String),
    pub(super) erasures: Option<Arc<dyn ErasureDrain>>,
    pub(super) lane: Option<LaneSupervisor>,
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
            blob_reaper: None,
            listener_queue_threshold: config.listener_queue_threshold,
            schema_version: (
                crate::schema::ENGINE_SCHEMA_VERSION.to_string(),
                config.schema_service_version().to_string(),
            ),
            erasures: None,
            lane: None,
        })
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
        self.refresh_schema_version(pg).await;
        let blobs = match &mut self.blob_reaper {
            Some(reaper) => reaper.sweep(pg).await,
            None => ReaperRound::default(),
        };
        if let Some(drain) = &self.erasures
            && let Err(error) = drain.drain().await
        {
            tracing::warn!(
                reason = %describe(&error),
                "the beat could not complete a pending person-erasure purge",
            );
        }
        if let Some(lane) = &mut self.lane {
            lane.tick().await;
        }
        let queue_usage = self.queue_usage().await;
        self.apply_listener_brake(queue_usage);
        if let Some(readiness) = &self.readiness {
            readiness.refresh();
        }
        let round = BeatRound {
            more: relays.more || scheduled.more,
            queue_usage,
            relays,
            cron,
            scheduled,
            gc,
            blobs,
        };
        crate::observe::record_beat(&round);
        round
    }

    fn apply_listener_brake(&self, queue_usage: Option<f64>) {
        if let (Some(transport), Some(usage)) = (&self.transport, queue_usage) {
            transport.set_brake(usage >= self.listener_queue_threshold);
        }
    }

    async fn refresh_schema_version(&self, pg: &PgPool) {
        let (engine_version, service_version) = &self.schema_version;
        if let Err(error) =
            crate::schema_version::refresh_schema_version(pg, engine_version, service_version).await
        {
            tracing::warn!(
                reason = %describe(&error),
                "the beat could not refresh the schema version heartbeat",
            );
        }
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
mod tests;
