use sqlx::{Executor, Postgres};
use uuid::Uuid;

use br_core_integration::{OutboxStatus, Transition};

pub const OUTBOX_TABLE: &str = "integration_outbox";
pub const OUTBOX_NOTIFY_CHANNEL: &str = "integration_outbox";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOutbox {
    pub id: Uuid,
    pub subject: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OutboxStore;

impl OutboxStore {
    pub fn new() -> Self {
        Self
    }

    pub async fn fetch_one_pending<'e, E>(
        &self,
        executor: E,
        after: Uuid,
    ) -> Result<Option<PendingOutbox>, sqlx::Error>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let row: Option<OutboxRow> = sqlx::query_as(
            "SELECT id, subject, payload, attempts \
             FROM integration_outbox \
             WHERE status = 'PENDING' AND id > $1 \
             ORDER BY id \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(after)
        .fetch_optional(executor)
        .await?;
        Ok(row.map(OutboxRow::into_pending))
    }

    pub async fn apply_transition<'e, E>(
        &self,
        executor: E,
        id: Uuid,
        transition: Transition,
        last_error: Option<&str>,
    ) -> Result<(), sqlx::Error>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let published_at_clause = if transition.status == OutboxStatus::Published {
            "published_at = NOW()"
        } else {
            "published_at = published_at"
        };
        let sql = format!(
            "UPDATE integration_outbox \
             SET status = $2, attempts = $3, last_error = $4, {published_at_clause} \
             WHERE id = $1"
        );
        sqlx::query(&sql)
            .bind(id)
            .bind(transition.status.as_db_str())
            .bind(i64::from(transition.attempts))
            .bind(last_error)
            .execute(executor)
            .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: Uuid,
    subject: String,
    payload: serde_json::Value,
    attempts: i64,
}

impl OutboxRow {
    fn into_pending(self) -> PendingOutbox {
        PendingOutbox {
            id: self.id,
            subject: self.subject,
            payload: self.payload,
            attempts: self.attempts.max(0) as u32,
        }
    }
}
