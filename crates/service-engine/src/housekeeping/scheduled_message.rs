use async_nats::HeaderMap;
use bytes::Bytes;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::EngineError;
use crate::inbound::HEADER_MESSAGE_ID;
use crate::nats::Nats;
use crate::schema::TABLE_SCHEDULED_MESSAGE;

pub const DEFAULT_SCHEDULED_MESSAGE_BATCH: i64 = 128;

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    subject: String,
    message_id: Uuid,
    payload: Vec<u8>,
}

pub async fn fire_due(pool: &PgPool, nats: &Nats, batch: i64) -> Result<usize, EngineError> {
    let mut tx = pool.begin().await?;
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT id, subject, message_id, payload FROM {TABLE_SCHEDULED_MESSAGE} \
         WHERE at <= now() ORDER BY at, id LIMIT $1 FOR UPDATE SKIP LOCKED"
    ))
    .bind(batch)
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }
    for row in &rows {
        publish(nats, row).await?;
    }
    let ids: Vec<Uuid> = rows.iter().map(|row| row.id).collect();
    sqlx::query(&format!(
        "DELETE FROM {TABLE_SCHEDULED_MESSAGE} WHERE id = ANY($1)"
    ))
    .bind(&ids)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows.len())
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
