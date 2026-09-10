mod report;
mod stage;
mod store;

pub use report::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_MESSAGES, FailureClass, RelayPass, RelayPolicy,
    classify_failure,
};
pub use stage::{OutboundSequence, OutboxRecord, event_subject, stage};
pub use store::{OUTBOX_NOTIFY_CHANNEL, OUTBOX_TABLE, OutboxStore, PendingOutbox};

use std::sync::Mutex;
use std::time::{Duration, Instant};

use br_core_integration::{OutboxStatus, Transition, next_after_attempt};
use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::config::DEFAULT_MESSAGE_RETENTION;
use crate::error::RelayError;
use crate::name::RelayName;
use crate::nats::{Nats, RelayHealthChannel, RelayHealthReceiver};
use crate::relay::{Claim, Discipline, Drained, Relay};
use crate::relays::outbox::report::{MessageIdSource, classify_pass, message_id_for};

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    pub published: u64,
    pub claims: u64,
}

pub struct OutboxRelay {
    pool: PgPool,
    nats: Nats,
    store: OutboxStore,
    policy: RelayPolicy,
    health: RelayHealthChannel,
}

impl OutboxRelay {
    pub fn new(pool: PgPool, nats: Nats) -> Self {
        Self::with(pool, nats, RelayPolicy::default())
    }

    pub fn with(pool: PgPool, nats: Nats, policy: RelayPolicy) -> Self {
        Self {
            pool,
            nats,
            store: OutboxStore::new(),
            policy,
            health: RelayHealthChannel::new(),
        }
    }

    pub fn health(&self) -> RelayHealthReceiver {
        self.health.receiver()
    }

    pub async fn sweep(&self, older_than: Duration) -> Result<Swept, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let published = self.store.sweep_published(&mut *conn, older_than).await?;
        let claims = self.store.sweep_claims(&mut *conn, older_than).await?;
        Ok(Swept { published, claims })
    }

    pub async fn run_once_detailed(&self) -> Result<RelayPass, sqlx::Error> {
        let mut pass = RelayPass::default();
        let cap = self.policy.max_messages.max(1);
        let mut cursor = Uuid::nil();
        for _ in 0..cap {
            match self.process_one(cursor, &mut pass).await? {
                Some(id) => cursor = id,
                None => break,
            }
        }
        Ok(pass)
    }

    async fn process_one(
        &self,
        after: Uuid,
        pass: &mut RelayPass,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let Some(record) = self.store.fetch_one_pending(&mut *tx, after).await? else {
            tx.commit().await?;
            return Ok(None);
        };

        let (message_id, id_source) = message_id_for(record.id, &record.payload);
        let sequence = match (&record.producer, &record.seq_key, record.seq) {
            (Some(producer), Some(seq_key), Some(seq)) => {
                Some((producer.as_str(), seq_key.as_str(), seq))
            }
            _ => None,
        };
        let publish_result = self
            .nats
            .publish_value_sequenced(
                &record.subject,
                &record.payload,
                &message_id.to_string(),
                sequence,
            )
            .await;

        let structural =
            publish_result.as_ref().err().map(classify_failure) == Some(FailureClass::Structural);

        let transition = if structural {
            Transition {
                status: OutboxStatus::Pending,
                attempts: record.attempts,
            }
        } else {
            next_after_attempt(
                record.attempts,
                self.policy.max_attempts,
                publish_result.is_ok(),
            )
        };
        let last_error = publish_result.as_ref().err().map(|e| e.to_string());

        self.store
            .apply_transition(&mut *tx, record.id, transition, last_error.as_deref())
            .await?;
        tx.commit().await?;

        pass.picked += 1;
        if id_source == MessageIdSource::Row {
            pass.row_id_fallbacks += 1;
        }
        classify_pass(pass, &publish_result, transition, structural);
        Ok(Some(record.id))
    }
}

pub struct HostedOutboxRelay {
    name: RelayName,
    hosted: OutboxRelay,
    cap: usize,
    last: Mutex<Option<RelayPass>>,
    retention: Duration,
    sweep_every: Duration,
    last_sweep: Mutex<Option<Instant>>,
    swept: Mutex<Swept>,
}

impl HostedOutboxRelay {
    pub fn hosting(name: RelayName, hosted: OutboxRelay) -> Self {
        let cap = hosted.policy.max_messages.max(1);
        Self {
            name,
            hosted,
            cap,
            last: Mutex::new(None),
            retention: DEFAULT_MESSAGE_RETENTION,
            sweep_every: DEFAULT_SWEEP_INTERVAL,
            last_sweep: Mutex::new(None),
            swept: Mutex::new(Swept::default()),
        }
    }

    pub fn with_message_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    pub fn with_sweep_every(mut self, sweep_every: Duration) -> Self {
        self.sweep_every = sweep_every;
        self
    }

    pub fn health(&self) -> RelayHealthReceiver {
        self.hosted.health()
    }

    pub fn last_pass(&self) -> Option<RelayPass> {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn swept(&self) -> Swept {
        *self
            .swept
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record(&self, pass: RelayPass) {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(pass);
    }

    fn sweep_due(&self) -> bool {
        let guard = self
            .last_sweep
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.is_none_or(|at| at.elapsed() >= self.sweep_every)
    }

    async fn maybe_sweep(&self) {
        if !self.sweep_due() {
            return;
        }
        match self.hosted.sweep(self.retention).await {
            Ok(swept) => {
                let mut total = self
                    .swept
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                total.published += swept.published;
                total.claims += swept.claims;
                *self
                    .last_sweep
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
            }
            Err(error) => {
                tracing::warn!(reason = %error, "outbox hygiene sweep failed");
            }
        }
    }

    async fn run(&self) -> Result<Drained, RelayError> {
        self.maybe_sweep().await;
        let pass = self.hosted.run_once_detailed().await?;
        self.record(pass);
        verdict(&self.name, pass.picked, pass.structural, self.cap)
    }
}

fn verdict(
    name: &RelayName,
    picked: usize,
    structural: usize,
    cap: usize,
) -> Result<Drained, RelayError> {
    if structural > 0 {
        return Err(RelayError::Publish(format!(
            "{name} left {structural} row(s) pending on a structural failure"
        )));
    }
    Ok(Drained::rows(picked, picked >= cap))
}

impl Relay for HostedOutboxRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::RowClaim
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(self.run())
    }

    fn hosted_drain<'a>(
        &'a self,
        _pg: &'a PgPool,
        _claim: &'a Claim,
    ) -> Option<BoxFuture<'a, Result<Drained, RelayError>>> {
        Some(Box::pin(self.run()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> RelayName {
        RelayName::from_static("integration_outbox")
    }

    #[test]
    fn a_pass_that_filled_its_cap_asks_the_beat_to_come_back_before_the_next_one() {
        assert_eq!(verdict(&name(), 4, 0, 4).unwrap(), Drained::rows(4, true));
        assert_eq!(verdict(&name(), 1, 0, 4).unwrap(), Drained::rows(1, false));
        assert_eq!(verdict(&name(), 0, 0, 4).unwrap(), Drained::NOTHING);
    }

    #[test]
    fn a_structural_failure_backs_the_relay_off_instead_of_reporting_a_clean_pass() {
        let error = verdict(&name(), 3, 1, 256).unwrap_err();
        assert!(matches!(error, RelayError::Publish(_)));
        assert!(error.to_string().contains("integration_outbox"));
    }
}
