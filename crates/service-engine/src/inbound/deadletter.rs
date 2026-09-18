use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use crate::impact::{Dims, Impact};
use crate::inbound::deadletter_row::{Row, republish};
use crate::inbound::message::Incoming;
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
    Outbox,
    Render,
}

impl DeadLetterSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reaction => "reaction",
            Self::Scheduled => "scheduled",
            Self::Cron => "cron",
            Self::Mirror => "mirror",
            Self::Outbox => "outbox",
            Self::Render => "render",
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

pub(crate) struct StagedDeadLetter<'a> {
    pub(crate) source: DeadLetterSource,
    pub(crate) reaction: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) message_id: Uuid,
    pub(crate) payload: &'a [u8],
    pub(crate) producer: Option<&'a str>,
    pub(crate) seq_key: Option<&'a str>,
    pub(crate) seq: Option<i64>,
    pub(crate) error: &'a str,
    pub(crate) delivered: i32,
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
                Some(s.key.producer.as_str()),
                Some(s.key.key.as_str()),
                Some(s.seq as i64),
            ),
            None => (None, None, None),
        };
        let entry = StagedDeadLetter {
            source,
            reaction: &msg.reaction,
            subject: &msg.subject,
            message_id: msg.message_id,
            payload: msg.payload.as_ref(),
            producer,
            seq_key,
            seq,
            error,
            delivered: msg.delivered as i32,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_work(
        &self,
        source: DeadLetterSource,
        reaction: &str,
        subject: &str,
        message_id: Uuid,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let entry = StagedDeadLetter {
            source,
            reaction,
            subject,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_mirror(
        &self,
        mirror: &str,
        subject: &str,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let message_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{mirror}\u{0}{subject}").as_bytes(),
        );
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Mirror,
            reaction: mirror,
            subject,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_render(
        &self,
        projector: &str,
        key: &str,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let message_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{projector}\u{0}{key}").as_bytes(),
        );
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Render,
            reaction: projector,
            subject: key,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub(crate) async fn stage(
        &self,
        conn: &mut sqlx::PgConnection,
        entry: &StagedDeadLetter<'_>,
    ) -> Result<bool, sqlx::Error> {
        let id = Uuid::now_v7();
        let sql = format!(
            "INSERT INTO {TABLE_DEAD_LETTER} \
               (id, source, reaction, subject, message_id, payload, producer, seq_key, seq, error, delivered) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (reaction, message_id) DO UPDATE \
               SET delivered = EXCLUDED.delivered, error = EXCLUDED.error, last_seen = now() \
             RETURNING id, (xmax = 0) AS inserted"
        );
        let (row_id, inserted): (Uuid, bool) = sqlx::query_as(&sql)
            .bind(id)
            .bind(entry.source.as_str())
            .bind(entry.reaction)
            .bind(entry.subject)
            .bind(entry.message_id)
            .bind(entry.payload)
            .bind(entry.producer)
            .bind(entry.seq_key)
            .bind(entry.seq)
            .bind(entry.error)
            .bind(entry.delivered)
            .fetch_one(&mut *conn)
            .await?;
        if inserted {
            crate::observe::record_dead_letter(entry.source.as_str());
        }
        let Some(transport) = &self.transport else {
            return Ok(false);
        };
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
            .stage_in(conn, std::slice::from_ref(&impact))
            .await
            .map_err(|error| {
                sqlx::Error::Protocol(format!("staging the ops-view impact: {error}"))
            })?;
        Ok(true)
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
