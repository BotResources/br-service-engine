use sqlx::PgPool;
use uuid::Uuid;

use crate::chain::describe;
use crate::error::EngineError;
use crate::inbound::{DeadLetterSource, DeadLetters, StagedDeadLetter};
use crate::nats::Nats;
use crate::relays::outbox::{FailureClass, classify_failure};
use crate::schema::TABLE_SCHEDULED_MESSAGE;

pub const DEFAULT_SCHEDULED_MESSAGE_BATCH: i64 = 128;
pub const DEFAULT_SCHEDULED_MESSAGE_BUDGET: u32 = 8;

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    subject: String,
    message_id: Uuid,
    payload: Vec<u8>,
    attempts: i32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledRound {
    pub fired: usize,
    pub dead_lettered: usize,
    pub retried: usize,
}

enum Step {
    Fired,
    DeadLettered,
    Retried,
    Exhausted,
    Halt,
}

pub async fn fire_due(
    pool: &PgPool,
    nats: &Nats,
    dead_letters: &DeadLetters,
    batch: i64,
    budget: u32,
) -> Result<ScheduledRound, EngineError> {
    if !nats.reachable() {
        return Ok(ScheduledRound::default());
    }
    let mut round = ScheduledRound::default();
    for _ in 0..batch.max(0) {
        match fire_one(pool, nats, dead_letters, budget).await? {
            Step::Fired => round.fired += 1,
            Step::DeadLettered => round.dead_lettered += 1,
            Step::Retried => round.retried += 1,
            Step::Exhausted | Step::Halt => break,
        }
    }
    Ok(round)
}

async fn fire_one(
    pool: &PgPool,
    nats: &Nats,
    dead_letters: &DeadLetters,
    budget: u32,
) -> Result<Step, EngineError> {
    let mut tx = pool.begin().await?;
    let Some(row): Option<Row> = sqlx::query_as(&format!(
        "SELECT id, subject, message_id, payload, attempts FROM {TABLE_SCHEDULED_MESSAGE} \
         WHERE at <= now() ORDER BY at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    ))
    .fetch_optional(&mut *tx)
    .await?
    else {
        tx.rollback().await?;
        return Ok(Step::Exhausted);
    };

    let error = match nats
        .publish_bytes_with_id(&row.subject, &row.payload, &row.message_id.to_string())
        .await
    {
        Ok(_) => {
            delete(&mut tx, row.id).await?;
            tx.commit().await?;
            return Ok(Step::Fired);
        }
        Err(error) => error,
    };

    if classify_failure(&error) == FailureClass::Unanswered || !nats.reachable() {
        tx.rollback().await?;
        return Ok(Step::Halt);
    }

    let attempts = row.attempts.saturating_add(1);
    if attempts as u32 >= budget {
        let detail = describe(&error);
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Scheduled,
            reaction: &row.subject,
            subject: &row.subject,
            message_id: row.message_id,
            payload: &row.payload,
            producer: None,
            seq_key: None,
            seq: None,
            error: &detail,
            delivered: attempts,
        };
        let staged = dead_letters.stage(&mut tx, &entry).await?;
        delete(&mut tx, row.id).await?;
        tx.commit().await?;
        if staged {
            crate::observe::record_impacts_committed(1);
        }
        tracing::warn!(
            subject = %row.subject,
            attempts,
            reason = %detail,
            "a scheduled message exhausted its delivery budget and was dead-lettered",
        );
        return Ok(Step::DeadLettered);
    }

    sqlx::query(&format!(
        "UPDATE {TABLE_SCHEDULED_MESSAGE} SET attempts = $2 WHERE id = $1"
    ))
    .bind(row.id)
    .bind(attempts)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Step::Retried)
}

async fn delete(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "DELETE FROM {TABLE_SCHEDULED_MESSAGE} WHERE id = $1"
    ))
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
