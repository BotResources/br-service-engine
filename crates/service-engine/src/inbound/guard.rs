use sqlx::PgConnection;
use uuid::Uuid;

use crate::inbound::message::SequenceKey;
use crate::schema::{TABLE_MESSAGE_CLAIM, TABLE_SEQUENCE_GUARD};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claimed {
    Fresh,
    Duplicate,
}

pub async fn claim(
    conn: &mut PgConnection,
    message_id: Uuid,
    reaction: &str,
) -> Result<Claimed, sqlx::Error> {
    let sql = format!(
        "INSERT INTO {TABLE_MESSAGE_CLAIM} (message_id, reaction) VALUES ($1, $2) \
         ON CONFLICT (message_id, reaction) DO NOTHING"
    );
    let done = sqlx::query(&sql)
        .bind(message_id)
        .bind(reaction)
        .execute(conn)
        .await?;
    Ok(if done.rows_affected() == 1 {
        Claimed::Fresh
    } else {
        Claimed::Duplicate
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Advanced,
    Stale,
}

pub async fn advance_sequence(
    conn: &mut PgConnection,
    reaction: &str,
    key: &SequenceKey,
    seq: u64,
) -> Result<Ordering, sqlx::Error> {
    let seq = i64::try_from(seq).map_err(|_| {
        sqlx::Error::Protocol(format!(
            "producer sequence {seq} exceeds the i64 ceiling of the sequence guard column"
        ))
    })?;
    let sql = format!(
        "INSERT INTO {TABLE_SEQUENCE_GUARD} (producer, reaction, seq_key, last_seq) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (producer, reaction, seq_key) DO UPDATE SET last_seq = EXCLUDED.last_seq \
         WHERE {TABLE_SEQUENCE_GUARD}.last_seq < EXCLUDED.last_seq \
         RETURNING last_seq"
    );
    let advanced: Option<(i64,)> = sqlx::query_as(&sql)
        .bind(&key.producer)
        .bind(reaction)
        .bind(&key.key)
        .bind(seq)
        .fetch_optional(conn)
        .await?;
    Ok(if advanced.is_some() {
        Ordering::Advanced
    } else {
        Ordering::Stale
    })
}
