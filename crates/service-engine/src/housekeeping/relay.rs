use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::watch;

use crate::chain::describe;
use crate::config::{DEFAULT_BEAT, DEFAULT_LEASE};
use crate::error::EngineError;
use crate::housekeeping::backoff::Backoff;
use crate::housekeeping::drain::drain_one;
use crate::housekeeping::health::{RelayCondition, RelaysHealth, RelaysHealthReceiver};
use crate::name::{PodId, RelayName};
use crate::relay::{Discipline, Relay};

pub const DEFAULT_BATCH: usize = 256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayRound {
    pub ran: usize,
    pub rows: usize,
    pub skipped: usize,
    pub failed: usize,
    pub slot_won: usize,
    pub slot_skipped: usize,
    pub more: bool,
}

struct Entry {
    name: RelayName,
    discipline: Discipline,
    relay: Arc<dyn Relay>,
    backoff: Backoff,
    reason: Option<String>,
}

pub struct RelayRuntime {
    pod: PodId,
    lease: Duration,
    slot_period: Duration,
    batch: usize,
    entries: Vec<Entry>,
    health: watch::Sender<RelaysHealth>,
    subscription: RelaysHealthReceiver,
}

impl RelayRuntime {
    pub fn new(pod: PodId) -> Self {
        let (health, subscription) = watch::channel(RelaysHealth::default());
        Self {
            pod,
            lease: DEFAULT_LEASE,
            slot_period: DEFAULT_BEAT,
            batch: DEFAULT_BATCH,
            entries: Vec::new(),
            health,
            subscription,
        }
    }

    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn with_slot_period(mut self, slot_period: Duration) -> Self {
        self.slot_period = slot_period;
        self
    }

    pub fn with_batch(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    pub fn register<R: Relay>(&mut self, relay: R) -> Result<(), EngineError> {
        self.register_erased(Arc::new(relay))
    }

    pub fn register_erased(&mut self, relay: Arc<dyn Relay>) -> Result<(), EngineError> {
        let name = relay.name();
        if self.entries.iter().any(|entry| entry.name == name) {
            return Err(EngineError::DuplicateRelayName { name });
        }
        let entry = Entry {
            name,
            discipline: relay.discipline(),
            relay,
            backoff: Backoff::default(),
            reason: None,
        };
        self.entries.push(entry);
        self.publish_health();
        Ok(())
    }

    pub fn names(&self) -> Vec<RelayName> {
        self.entries.iter().map(|e| e.name.clone()).collect()
    }

    pub fn health(&self) -> RelaysHealthReceiver {
        self.subscription.clone()
    }

    pub async fn beat(&mut self, pg: &PgPool) -> RelayRound {
        self.drain(pg, None).await
    }

    pub async fn after_pass(&mut self, pg: &PgPool) -> RelayRound {
        self.drain(pg, Some(Discipline::RowClaim)).await
    }

    async fn drain(&mut self, pg: &PgPool, only: Option<Discipline>) -> RelayRound {
        let mut round = RelayRound::default();
        for index in 0..self.entries.len() {
            let now = Instant::now();
            let entry = &self.entries[index];
            if only.is_some_and(|d| d != entry.discipline) || !entry.backoff.is_ready(now) {
                round.skipped += 1;
                continue;
            }
            let outcome = drain_one(
                pg,
                entry.relay.clone(),
                entry.discipline,
                &self.pod,
                self.batch,
                self.lease,
                self.slot_period,
            )
            .await;
            let entry = &mut self.entries[index];
            let leader = entry.discipline == Discipline::Leader;
            match outcome {
                Ok(Some(drained)) => {
                    entry.backoff.succeed();
                    entry.reason = None;
                    round.ran += 1;
                    round.rows += drained.rows;
                    round.more |= drained.more;
                    if leader {
                        round.slot_won += 1;
                    }
                }
                Ok(None) => {
                    round.skipped += 1;
                    if leader {
                        round.slot_skipped += 1;
                    }
                }
                Err(error) => {
                    let wait = entry.backoff.fail(Instant::now());
                    let reason = describe(&error);
                    round.failed += 1;
                    tracing::warn!(
                        relay = %entry.name,
                        attempts = entry.backoff.attempts(),
                        retry_in_ms = wait.as_millis(),
                        reason = %reason,
                        "relay drain failed",
                    );
                    entry.reason = Some(reason);
                }
            }
        }
        self.publish_health();
        round
    }

    fn publish_health(&self) {
        let now = Instant::now();
        let board: RelaysHealth = self
            .entries
            .iter()
            .map(|entry| {
                let condition = if entry.backoff.is_ready(now) {
                    RelayCondition::Healthy
                } else {
                    RelayCondition::BackingOff {
                        attempts: entry.backoff.attempts(),
                        retry_in: entry.backoff.retry_in(now),
                        reason: entry.reason.clone().unwrap_or_default(),
                    }
                };
                (entry.name.clone(), condition)
            })
            .collect();
        let _ = self.health.send(board);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RelayError;
    use crate::relay::{Claim, Drained};

    fn pod() -> PodId {
        PodId::new("svc-sample-0").unwrap()
    }

    #[test]
    fn a_runtime_with_no_relay_reports_a_healthy_empty_board() {
        let runtime = RelayRuntime::new(pod());
        let board = runtime.health().borrow().clone();
        assert!(board.is_empty());
        assert!(board.is_healthy());
        assert!(runtime.names().is_empty());
    }

    #[test]
    fn a_registered_relay_appears_on_the_board_as_healthy_before_it_has_ever_run() {
        struct Idle;
        impl Relay for Idle {
            fn name(&self) -> RelayName {
                RelayName::from_static("idle")
            }
            fn drain<'a>(
                &'a self,
                _conn: &'a mut sqlx::PgConnection,
                _claim: &'a Claim,
            ) -> futures_util::future::BoxFuture<'a, Result<Drained, RelayError>> {
                Box::pin(async { Ok(Drained::NOTHING) })
            }
        }
        let mut runtime = RelayRuntime::new(pod());
        runtime.register(Idle).expect("the first relay registers");
        let board = runtime.health().borrow().clone();
        assert_eq!(runtime.names(), vec![RelayName::from_static("idle")]);
        assert_eq!(
            board.condition(&RelayName::from_static("idle")),
            Some(&RelayCondition::Healthy)
        );
        assert!(board.degraded().next().is_none());
    }

    #[test]
    fn two_relays_claiming_one_name_would_share_a_health_condition_so_the_second_is_refused() {
        struct Named(&'static str);
        impl Relay for Named {
            fn name(&self) -> RelayName {
                RelayName::from_static(self.0)
            }
            fn drain<'a>(
                &'a self,
                _conn: &'a mut sqlx::PgConnection,
                _claim: &'a Claim,
            ) -> futures_util::future::BoxFuture<'a, Result<Drained, RelayError>> {
                Box::pin(async { Ok(Drained::NOTHING) })
            }
        }
        let mut runtime = RelayRuntime::new(pod());
        runtime
            .register(Named("outbox"))
            .expect("the first relay registers");
        let refusal = runtime.register(Named("outbox"));
        assert!(matches!(
            refusal,
            Err(EngineError::DuplicateRelayName { name }) if name.as_str() == "outbox"
        ));
        assert_eq!(
            runtime.names(),
            vec![RelayName::from_static("outbox")],
            "the refused duplicate never enters the board, so one down relay cannot hide behind \
             another's condition"
        );
    }
}
