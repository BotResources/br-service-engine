use sqlx::{Executor, Postgres};
use uuid::Uuid;

use br_core_integration::{EventCoords, IntegrationEvent, OutboxStatus};

pub use crate::nats::event_subject;

use crate::relays::outbox::store::OUTBOX_NOTIFY_CHANNEL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: Uuid,
    pub subject: String,
    pub payload: serde_json::Value,
    pub status: OutboxStatus,
    pub attempts: u32,
}

impl OutboxRecord {
    pub fn stage(id: Uuid, destination: &EventCoords, payload: serde_json::Value) -> Self {
        Self {
            id,
            subject: event_subject(destination),
            payload,
            status: OutboxStatus::Pending,
            attempts: 0,
        }
    }

    pub fn stage_event<T: serde::Serialize>(
        id: Uuid,
        destination: &EventCoords,
        event: &IntegrationEvent<T>,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::stage(id, destination, serde_json::to_value(event)?))
    }

    pub fn stage_to(id: Uuid, subject: String, payload: serde_json::Value) -> Self {
        Self {
            id,
            subject,
            payload,
            status: OutboxStatus::Pending,
            attempts: 0,
        }
    }
}

pub async fn stage<'e, E>(executor: E, record: &OutboxRecord) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        "WITH inserted AS ( \
            INSERT INTO integration_outbox (id, subject, payload, status, attempts) \
            VALUES ($1, $2, $3, $4, $5) \
            ON CONFLICT (id) DO NOTHING \
            RETURNING 1 \
         ) \
         SELECT pg_notify($6, '')",
    )
    .bind(record.id)
    .bind(&record.subject)
    .bind(&record.payload)
    .bind(record.status.as_db_str())
    .bind(i64::from(record.attempts))
    .bind(OUTBOX_NOTIFY_CHANNEL)
    .execute(executor)
    .await?;
    Ok(())
}
