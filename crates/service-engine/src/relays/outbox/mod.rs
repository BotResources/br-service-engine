mod adopt;
mod hosted;
mod report;
mod stage;
mod store;

pub use adopt::adopt_legacy_outbox;
pub use hosted::{DEFAULT_SWEEP_INTERVAL, HostedOutboxRelay};
pub use report::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_MESSAGES, FailureClass, RelayPass, RelayPolicy,
    classify_failure,
};
pub use stage::{OutboundSequence, OutboxRecord, event_subject, stage};
pub use store::{OUTBOX_NOTIFY_CHANNEL, OUTBOX_TABLE, OutboxStore, PendingOutbox};

use std::time::Duration;

use br_core_integration::{OutboxStatus, Transition, next_after_attempt};
use sqlx::PgPool;
use uuid::Uuid;

use crate::inbound::{DeadLetterSource, DeadLetters, StagedDeadLetter};
use crate::nats::{Nats, RelayHealthChannel, RelayHealthReceiver};
use crate::relays::outbox::report::{MessageIdSource, message_id_for};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    pub published: u64,
    pub failed: u64,
    pub claims: u64,
}

enum Step {
    Advanced(Uuid),
    Exhausted,
    Halt,
}

pub struct OutboxRelay {
    pool: PgPool,
    nats: Nats,
    store: OutboxStore,
    policy: RelayPolicy,
    health: RelayHealthChannel,
    dead_letters: Option<DeadLetters>,
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
            dead_letters: None,
        }
    }

    pub fn with_dead_letters(mut self, dead_letters: DeadLetters) -> Self {
        self.dead_letters = Some(dead_letters);
        self
    }

    pub fn health(&self) -> RelayHealthReceiver {
        self.health.receiver()
    }

    pub fn cap(&self) -> usize {
        self.policy.max_messages.max(1)
    }

    pub async fn sweep(&self, older_than: Duration) -> Result<Swept, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let published = self.store.sweep_published(&mut *conn, older_than).await?;
        let failed = self.store.sweep_failed(&mut *conn, older_than).await?;
        let claims = self.store.sweep_claims(&mut *conn, older_than).await?;
        Ok(Swept {
            published,
            failed,
            claims,
        })
    }

    pub async fn pending_stats(&self) -> Result<(i64, Option<f64>), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.store.pending_stats(&mut *conn).await
    }

    pub async fn run_once_detailed(&self) -> Result<RelayPass, sqlx::Error> {
        let mut pass = RelayPass::default();
        if !self.nats.reachable() {
            return Ok(pass);
        }
        let cap = self.policy.max_messages.max(1);
        let mut cursor = Uuid::nil();
        for _ in 0..cap {
            match self.process_one(cursor, &mut pass).await? {
                Step::Advanced(id) => cursor = id,
                Step::Exhausted | Step::Halt => break,
            }
        }
        Ok(pass)
    }

    async fn process_one(&self, after: Uuid, pass: &mut RelayPass) -> Result<Step, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let Some(record) = self.store.fetch_one_pending(&mut *tx, after).await? else {
            tx.commit().await?;
            return Ok(Step::Exhausted);
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

        let outcome = match publish_result {
            Ok(outcome) => {
                let transition =
                    next_after_attempt(record.attempts, self.policy.max_attempts, true);
                self.store
                    .apply_transition(&mut *tx, record.id, transition, None)
                    .await?;
                tx.commit().await?;
                pass.picked += 1;
                if id_source == MessageIdSource::Row {
                    pass.row_id_fallbacks += 1;
                }
                pass.published += 1;
                if outcome.is_duplicate() {
                    pass.duplicates += 1;
                }
                return Ok(Step::Advanced(record.id));
            }
            Err(error) => error,
        };

        let detail = outcome.to_string();
        if classify_failure(&outcome) == FailureClass::Structural {
            self.store
                .apply_transition(
                    &mut *tx,
                    record.id,
                    Transition {
                        status: OutboxStatus::Pending,
                        attempts: record.attempts,
                    },
                    Some(&detail),
                )
                .await?;
            tx.commit().await?;
            pass.picked += 1;
            pass.structural += 1;
            return Ok(Step::Halt);
        }

        if classify_failure(&outcome) == FailureClass::Unanswered || !self.nats.reachable() {
            let _ = tx.rollback().await;
            return Ok(Step::Halt);
        }

        let transition = next_after_attempt(record.attempts, self.policy.max_attempts, false);
        if transition.status == OutboxStatus::Failed {
            if self.dead_letters.is_some() {
                self.dead_letter(&mut tx, &record, message_id, transition.attempts, &detail)
                    .await?;
                self.store
                    .apply_transition(&mut *tx, record.id, transition, Some(&detail))
                    .await?;
                tx.commit().await?;
                pass.picked += 1;
                pass.failed += 1;
                pass.dead_lettered += 1;
                return Ok(Step::Halt);
            }
            self.store
                .apply_transition(
                    &mut *tx,
                    record.id,
                    Transition {
                        status: OutboxStatus::Pending,
                        attempts: record.attempts,
                    },
                    Some(&detail),
                )
                .await?;
            tx.commit().await?;
            pass.picked += 1;
            pass.retried += 1;
            return Ok(Step::Halt);
        }

        self.store
            .apply_transition(&mut *tx, record.id, transition, Some(&detail))
            .await?;
        tx.commit().await?;
        pass.picked += 1;
        pass.retried += 1;
        pass.min_retry_attempts = Some(match pass.min_retry_attempts {
            Some(prev) => prev.min(transition.attempts),
            None => transition.attempts,
        });
        Ok(Step::Halt)
    }

    async fn dead_letter(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        record: &PendingOutbox,
        message_id: Uuid,
        attempts: u32,
        detail: &str,
    ) -> Result<(), sqlx::Error> {
        let Some(dead_letters) = &self.dead_letters else {
            return Ok(());
        };
        let payload = serde_json::to_vec(&record.payload).map_err(|error| {
            sqlx::Error::Protocol(format!("encoding a dead-lettered outbox payload: {error}"))
        })?;
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Outbox,
            reaction: &record.subject,
            subject: &record.subject,
            message_id,
            payload: &payload,
            producer: record.producer.as_deref(),
            seq_key: record.seq_key.as_deref(),
            seq: record.seq,
            error: detail,
            delivered: i32::try_from(attempts).unwrap_or(i32::MAX),
        };
        dead_letters.stage(tx, &entry).await?;
        Ok(())
    }
}
