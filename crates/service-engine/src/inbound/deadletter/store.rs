use uuid::Uuid;

use crate::impact::{Dims, Impact};
use crate::inbound::deadletter::{
    DEAD_LETTER_NOUN, DeadLetter, DeadLetterSource, DeadLetters, DiscardOutcome, RetryOutcome,
    StagedDeadLetter,
};
use crate::inbound::deadletter_row::{Row, republish};
use crate::nats::{Nats, NatsError};
use crate::schema::TABLE_DEAD_LETTER;
use crate::wire::KeyBytes;

impl DeadLetters {
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
            sqlx::Error::Protocol(format!(
                "encoding the ops-view impact key: {}",
                crate::chain::describe(&error)
            ))
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
                sqlx::Error::Protocol(format!(
                    "staging the ops-view impact: {}",
                    crate::chain::describe(&error)
                ))
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
                detail: crate::chain::describe(&e),
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
                detail: crate::chain::describe(&e),
            })?;
        Ok(RetryOutcome::Republished)
    }
}
