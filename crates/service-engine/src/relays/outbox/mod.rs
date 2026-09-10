mod hosted;
mod report;
mod stage;
mod store;

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

use crate::nats::{Nats, RelayHealthChannel, RelayHealthReceiver};
use crate::relays::outbox::report::{MessageIdSource, classify_pass, message_id_for};

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

    pub fn cap(&self) -> usize {
        self.policy.max_messages.max(1)
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
