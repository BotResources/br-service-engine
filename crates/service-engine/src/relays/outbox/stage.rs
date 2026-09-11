use sqlx::{Executor, Postgres};
use uuid::Uuid;

use br_core_integration::{EventCoords, IntegrationEvent, OutboxStatus};

pub use crate::nats::event_subject;

use crate::relays::outbox::store::OUTBOX_NOTIFY_CHANNEL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundSequence {
    pub producer: String,
    pub seq_key: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: Uuid,
    pub subject: String,
    pub payload: serde_json::Value,
    pub status: OutboxStatus,
    pub attempts: u32,
    pub sequence: Option<OutboundSequence>,
}

impl OutboxRecord {
    pub fn stage(id: Uuid, destination: &EventCoords, payload: serde_json::Value) -> Self {
        Self::stage_to(id, event_subject(destination), payload, None)
    }

    pub fn stage_event<T: serde::Serialize>(
        id: Uuid,
        destination: &EventCoords,
        event: &IntegrationEvent<T>,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::stage(id, destination, serde_json::to_value(event)?))
    }

    pub fn stage_to(
        id: Uuid,
        subject: String,
        payload: serde_json::Value,
        sequence: Option<OutboundSequence>,
    ) -> Self {
        Self {
            id,
            subject,
            payload,
            status: OutboxStatus::Pending,
            attempts: 0,
            sequence,
        }
    }
}

pub async fn stage<'e, E>(executor: E, record: &OutboxRecord) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let seq = record.sequence.as_ref().map(|s| i64::try_from(s.seq));
    let seq = match seq {
        Some(Ok(seq)) => Some(seq),
        Some(Err(_)) => {
            return Err(sqlx::Error::Protocol(
                "outbound producer sequence exceeds the i64 ceiling of the outbox column".into(),
            ));
        }
        None => None,
    };
    sqlx::query(
        "WITH inserted AS ( \
            INSERT INTO integration_outbox (id, subject, payload, status, attempts, producer, seq_key, seq) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
            ON CONFLICT (id) DO NOTHING \
            RETURNING 1 \
         ) \
         SELECT pg_notify($9, '')",
    )
    .bind(record.id)
    .bind(&record.subject)
    .bind(&record.payload)
    .bind(record.status.as_db_str())
    .bind(i64::from(record.attempts))
    .bind(record.sequence.as_ref().map(|s| s.producer.clone()))
    .bind(record.sequence.as_ref().map(|s| s.seq_key.clone()))
    .bind(seq)
    .bind(OUTBOX_NOTIFY_CHANNEL)
    .execute(executor)
    .await?;
    Ok(())
}
