use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::chain::describe;
use crate::error::EngineError;
use crate::housekeeping::leader::{sweep_abandoned_slots, sweep_completed_slots};
use crate::time::{self, Timestamp};

pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

pub trait SessionGc: Send + Sync + 'static {
    fn collect<'a>(&'a self, now: Timestamp) -> BoxFuture<'a, Result<usize, EngineError>>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcRound {
    pub ran: bool,
    pub completed_slots: u64,
    pub abandoned_slots: u64,
    pub seal_markers: u64,
    pub chunks: u64,
    pub sessions: usize,
    pub failures: usize,
}

pub struct Gc {
    interval: Duration,
    due_at: Option<Instant>,
    accumulators: Option<Arc<AccumulatorRuntime>>,
    sessions: Option<Arc<dyn SessionGc>>,
}

impl Default for Gc {
    fn default() -> Self {
        Self::new()
    }
}

impl Gc {
    pub fn new() -> Self {
        Self {
            interval: DEFAULT_GC_INTERVAL,
            due_at: None,
            accumulators: None,
            sessions: None,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.set_interval(interval);
        self
    }

    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    pub fn with_accumulators(mut self, accumulators: Arc<AccumulatorRuntime>) -> Self {
        self.accumulators = Some(accumulators);
        self
    }

    pub fn with_sessions(mut self, sessions: Arc<dyn SessionGc>) -> Self {
        self.sessions = Some(sessions);
        self
    }

    pub fn set_sessions(&mut self, sessions: Arc<dyn SessionGc>) {
        self.sessions = Some(sessions);
    }

    pub const fn interval(&self) -> Duration {
        self.interval
    }

    pub fn is_due(&self, now: Instant) -> bool {
        self.due_at.is_none_or(|at| now >= at)
    }

    pub async fn sweep(&mut self, pg: &PgPool, slot_retention: Duration) -> GcRound {
        let mut round = GcRound::default();
        if !self.is_due(Instant::now()) {
            return round;
        }
        self.due_at = Some(Instant::now() + self.interval);
        round.ran = true;
        let now = time::now();
        match sweep_slots(pg, slot_retention).await {
            Ok((completed, abandoned)) => {
                round.completed_slots = completed;
                round.abandoned_slots = abandoned;
            }
            Err(error) => fail(&mut round, "leader slots", &error),
        }
        if let Some(accumulators) = &self.accumulators {
            match accumulators.sweep_expired(now).await {
                Ok(swept) => {
                    round.seal_markers = swept.markers;
                    round.chunks = swept.chunks;
                }
                Err(error) => fail(&mut round, "accumulator chunks", &error),
            }
        }
        if let Some(sessions) = &self.sessions {
            match sessions.collect(now).await {
                Ok(collected) => round.sessions = collected,
                Err(error) => fail(&mut round, "sessions", &error),
            }
        }
        round
    }
}

async fn sweep_slots(pg: &PgPool, slot_retention: Duration) -> Result<(u64, u64), EngineError> {
    let mut conn = pg.acquire().await?;
    let completed = sweep_completed_slots(&mut conn, slot_retention).await?;
    let abandoned = sweep_abandoned_slots(&mut conn, slot_retention).await?;
    Ok((completed, abandoned))
}

fn fail(round: &mut GcRound, what: &'static str, error: &EngineError) {
    round.failures += 1;
    tracing::warn!(what, reason = %describe(error), "a housekeeping sweep failed");
}

impl std::fmt::Debug for Gc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gc")
            .field("interval", &self.interval)
            .field("accumulators", &self.accumulators.is_some())
            .field("sessions", &self.sessions.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sweep_that_just_ran_is_not_due_again_until_its_interval_has_passed() {
        let now = Instant::now();
        let mut gc = Gc::new().with_interval(Duration::from_secs(60));
        assert!(gc.is_due(now), "the first beat sweeps");
        gc.due_at = Some(now + gc.interval);
        assert!(!gc.is_due(now));
        assert!(!gc.is_due(now + Duration::from_secs(59)));
        assert!(gc.is_due(now + Duration::from_secs(60)));
    }

    #[test]
    fn a_zero_interval_sweeps_on_every_beat() {
        let now = Instant::now();
        let mut gc = Gc::new().with_interval(Duration::ZERO);
        gc.due_at = Some(now + gc.interval);
        assert!(gc.is_due(now));
    }
}
