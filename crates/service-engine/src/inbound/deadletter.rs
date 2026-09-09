use std::sync::Arc;

use async_nats::HeaderMap;
use bytes::Bytes;
use sqlx::PgPool;
use uuid::Uuid;

use crate::impact::{Dims, Impact};
use crate::inbound::message::{
    HEADER_MESSAGE_ID, HEADER_PRODUCER, HEADER_SEQ, HEADER_SEQ_KEY, Incoming,
};
use crate::name::NounName;
use crate::nats::{Nats, NatsError};
use crate::schema::TABLE_DEAD_LETTER;
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

pub const DEAD_LETTER_NOUN: NounName = NounName::from_static("service_engine_dead_letter");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterSource {
    Reaction,
    Scheduled,
    Cron,
    Mirror,
}

impl DeadLetterSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reaction => "reaction",
            Self::Scheduled => "scheduled",
            Self::Cron => "cron",
            Self::Mirror => "mirror",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: Uuid,
    pub source: String,
    pub reaction: String,
    pub subject: String,
    pub message_id: Uuid,
    pub payload: Vec<u8>,
    pub producer: Option<String>,
    pub seq_key: Option<String>,
    pub seq: Option<i64>,
    pub error: String,
    pub delivered: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    Republished,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardOutcome {
    Discarded,
    Absent,
}

#[derive(Clone)]
pub struct DeadLetters {
    pool: PgPool,
    transport: Option<Arc<dyn ImpactTransport>>,
}

impl DeadLetters {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            transport: None,
        }
    }

    pub fn with_transport(mut self, transport: Arc<dyn ImpactTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub async fn record(
        &self,
        source: DeadLetterSource,
        msg: &Incoming,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let (producer, seq_key, seq) = match &msg.sequence {
            Some(s) => (
                Some(s.key.producer.clone()),
                Some(s.key.key.clone()),
                Some(s.seq as i64),
            ),
            None => (None, None, None),
        };
        let id = Uuid::now_v7();
        let sql = format!(
            "INSERT INTO {TABLE_DEAD_LETTER} \
               (id, source, reaction, subject, message_id, payload, producer, seq_key, seq, error, delivered) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (reaction, message_id) DO UPDATE \
               SET delivered = EXCLUDED.delivered, error = EXCLUDED.error, last_seen = now() \
             RETURNING id"
        );
        let mut tx = self.pool.begin().await?;
        let row_id: Uuid = sqlx::query_scalar(&sql)
            .bind(id)
            .bind(source.as_str())
            .bind(&msg.reaction)
            .bind(&msg.subject)
            .bind(msg.message_id)
            .bind(msg.payload.as_ref())
            .bind(producer)
            .bind(seq_key)
            .bind(seq)
            .bind(error)
            .bind(msg.delivered as i32)
            .fetch_one(&mut *tx)
            .await?;
        let mut committed_impacts = 0usize;
        if let Some(transport) = &self.transport {
            let key = KeyBytes::encode(&row_id).map_err(|error| {
                sqlx::Error::Protocol(format!("encoding the ops-view impact key: {error}"))
            })?;
            let impact = Impact::ResourceChanged {
                noun: DEAD_LETTER_NOUN,
                key,
                dims: Dims::ALL,
                cause: None,
            };
            transport
                .stage_in(&mut tx, std::slice::from_ref(&impact))
                .await
                .map_err(|error| {
                    sqlx::Error::Protocol(format!("staging the ops-view impact: {error}"))
                })?;
            committed_impacts = 1;
        }
        tx.commit().await?;
        crate::observe::record_impacts_committed(committed_impacts);
        Ok(())
    }

    pub async fn list(
        &self,
        source: Option<DeadLetterSource>,
        limit: i64,
    ) -> Result<Vec<DeadLetter>, sqlx::Error> {
        let sql = format!(
            "SELECT id, source, reaction, subject, message_id, payload, producer, seq_key, seq, error, delivered \
             FROM {TABLE_DEAD_LETTER} \
             WHERE ($1::text IS NULL OR source = $1) \
             ORDER BY first_seen \
             LIMIT $2"
        );
        let rows: Vec<Row> = sqlx::query_as(&sql)
            .bind(source.map(|s| s.as_str()))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Row::into_dead_letter).collect())
    }

    pub async fn discard(&self, id: Uuid) -> Result<DiscardOutcome, sqlx::Error> {
        let sql = format!("DELETE FROM {TABLE_DEAD_LETTER} WHERE id = $1");
        let done = sqlx::query(&sql).bind(id).execute(&self.pool).await?;
        Ok(if done.rows_affected() == 1 {
            DiscardOutcome::Discarded
        } else {
            DiscardOutcome::Absent
        })
    }

    pub async fn retry(&self, id: Uuid, nats: &Nats) -> Result<RetryOutcome, NatsError> {
        let sql = format!(
            "SELECT id, source, reaction, subject, message_id, payload, producer, seq_key, seq, error, delivered \
             FROM {TABLE_DEAD_LETTER} WHERE id = $1"
        );
        let row: Option<Row> = sqlx::query_as(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| NatsError::Store {
                detail: e.to_string(),
            })?;
        let Some(row) = row else {
            return Ok(RetryOutcome::Absent);
        };
        let entry = row.into_dead_letter();
        republish(nats, &entry).await?;
        let delete = format!("DELETE FROM {TABLE_DEAD_LETTER} WHERE id = $1");
        sqlx::query(&delete)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| NatsError::Store {
                detail: e.to_string(),
            })?;
        Ok(RetryOutcome::Republished)
    }
}

async fn republish(nats: &Nats, entry: &DeadLetter) -> Result<(), NatsError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_ID,
        Uuid::now_v7().to_string(),
    );
    headers.insert(HEADER_MESSAGE_ID, entry.message_id.to_string());
    if let (Some(producer), Some(seq_key), Some(seq)) = (&entry.producer, &entry.seq_key, entry.seq)
    {
        headers.insert(HEADER_PRODUCER, producer.as_str());
        headers.insert(HEADER_SEQ_KEY, seq_key.as_str());
        headers.insert(HEADER_SEQ, seq.to_string());
    }
    let ack = nats
        .context()
        .publish_with_headers(
            entry.subject.clone(),
            headers,
            Bytes::from(entry.payload.clone()),
        )
        .await
        .map_err(|e| NatsError::Publish {
            subject: entry.subject.clone(),
            kind: crate::nats::PublishFailure::Transient,
            detail: e.to_string(),
        })?;
    ack.await.map_err(|e| NatsError::Publish {
        subject: entry.subject.clone(),
        kind: crate::nats::PublishFailure::Transient,
        detail: e.to_string(),
    })?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    source: String,
    reaction: String,
    subject: String,
    message_id: Uuid,
    payload: Vec<u8>,
    producer: Option<String>,
    seq_key: Option<String>,
    seq: Option<i64>,
    error: String,
    delivered: i32,
}

impl Row {
    fn into_dead_letter(self) -> DeadLetter {
        DeadLetter {
            id: self.id,
            source: self.source,
            reaction: self.reaction,
            subject: self.subject,
            message_id: self.message_id,
            payload: self.payload,
            producer: self.producer,
            seq_key: self.seq_key,
            seq: self.seq,
            error: self.error,
            delivered: self.delivered,
        }
    }
}
