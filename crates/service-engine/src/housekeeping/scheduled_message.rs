use async_nats::HeaderMap;
use bytes::Bytes;
use sqlx::PgPool;
use uuid::Uuid;

use crate::chain::describe;
use crate::error::EngineError;
use crate::inbound::{DeadLetterSource, DeadLetters, HEADER_MESSAGE_ID, StagedDeadLetter};
use crate::nats::Nats;
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

pub async fn fire_due(
    pool: &PgPool,
    nats: &Nats,
    dead_letters: &DeadLetters,
    batch: i64,
    budget: u32,
) -> Result<ScheduledRound, EngineError> {
    let mut tx = pool.begin().await?;
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT id, subject, message_id, payload, attempts FROM {TABLE_SCHEDULED_MESSAGE} \
         WHERE at <= now() ORDER BY at, id LIMIT $1 FOR UPDATE SKIP LOCKED"
    ))
    .bind(batch)
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(ScheduledRound::default());
    }
    let mut round = ScheduledRound::default();
    let mut delete_ids: Vec<Uuid> = Vec::new();
    let mut retry: Vec<(Uuid, i32)> = Vec::new();
    let mut staged_impacts = 0usize;
    for row in &rows {
        match publish(nats, row).await {
            Ok(()) => {
                delete_ids.push(row.id);
                round.fired += 1;
            }
            Err(error) => {
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
                    if dead_letters.stage(&mut tx, &entry).await? {
                        staged_impacts += 1;
                    }
                    delete_ids.push(row.id);
                    round.dead_lettered += 1;
                    tracing::warn!(
                        subject = %row.subject,
                        attempts,
                        reason = %detail,
                        "a scheduled message exhausted its delivery budget and was dead-lettered; \
                         the following rows still fire",
                    );
                } else {
                    retry.push((row.id, attempts));
                    round.retried += 1;
                }
            }
        }
    }
    if !delete_ids.is_empty() {
        sqlx::query(&format!(
            "DELETE FROM {TABLE_SCHEDULED_MESSAGE} WHERE id = ANY($1)"
        ))
        .bind(&delete_ids)
        .execute(&mut *tx)
        .await?;
    }
    for (id, attempts) in &retry {
        sqlx::query(&format!(
            "UPDATE {TABLE_SCHEDULED_MESSAGE} SET attempts = $2 WHERE id = $1"
        ))
        .bind(id)
        .bind(attempts)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    crate::observe::record_impacts_committed(staged_impacts);
    Ok(round)
}

async fn publish(nats: &Nats, row: &Row) -> Result<(), EngineError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_ID,
        row.message_id.to_string(),
    );
    headers.insert(HEADER_MESSAGE_ID, row.message_id.to_string());
    let ack = nats
        .context()
        .publish_with_headers(
            row.subject.clone(),
            headers,
            Bytes::from(row.payload.clone()),
        )
        .await
        .map_err(|error| crate::nats::NatsError::Publish {
            subject: row.subject.clone(),
            kind: crate::nats::PublishFailure::Transient,
            detail: error.to_string(),
        })?;
    ack.await.map_err(|error| crate::nats::NatsError::Publish {
        subject: row.subject.clone(),
        kind: crate::nats::PublishFailure::Transient,
        detail: error.to_string(),
    })?;
    Ok(())
}
