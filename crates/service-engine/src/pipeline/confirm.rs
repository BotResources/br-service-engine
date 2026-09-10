use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::error::EngineError;
use crate::relays::outbox::{OutboundSequence, OutboxRecord};
use crate::schema::TABLE_MESSAGE_CLAIM;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredSequence {
    producer: String,
    seq_key: String,
    seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredConfirmation {
    subject: String,
    payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence: Option<StoredSequence>,
}

impl StoredConfirmation {
    fn from_record(record: &OutboxRecord) -> Self {
        Self {
            subject: record.subject.clone(),
            payload: record.payload.clone(),
            sequence: record.sequence.as_ref().map(|s| StoredSequence {
                producer: s.producer.clone(),
                seq_key: s.seq_key.clone(),
                seq: s.seq,
            }),
        }
    }

    fn into_record(self) -> OutboxRecord {
        let sequence = self.sequence.map(|s| OutboundSequence {
            producer: s.producer,
            seq_key: s.seq_key,
            seq: s.seq,
        });
        OutboxRecord::stage_to(Uuid::now_v7(), self.subject, self.payload, sequence)
    }
}

pub(crate) async fn record_confirmations(
    conn: &mut PgConnection,
    message_id: Uuid,
    reaction: &str,
    records: &[OutboxRecord],
) -> Result<(), EngineError> {
    if records.is_empty() {
        return Ok(());
    }
    let stored: Vec<StoredConfirmation> = records
        .iter()
        .map(StoredConfirmation::from_record)
        .collect();
    let value = serde_json::to_value(&stored).map_err(|source| EngineError::Encode {
        what: "stored confirmation",
        source,
    })?;
    sqlx::query(&format!(
        "UPDATE {TABLE_MESSAGE_CLAIM} SET confirmations = $3 \
         WHERE message_id = $1 AND reaction = $2"
    ))
    .bind(message_id)
    .bind(reaction)
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn stored_confirmations(
    conn: &mut PgConnection,
    message_id: Uuid,
    reaction: &str,
) -> Result<Vec<OutboxRecord>, EngineError> {
    let stored: Option<serde_json::Value> = sqlx::query_scalar(&format!(
        "SELECT confirmations FROM {TABLE_MESSAGE_CLAIM} \
         WHERE message_id = $1 AND reaction = $2"
    ))
    .bind(message_id)
    .bind(reaction)
    .fetch_optional(conn)
    .await?
    .flatten();
    let Some(value) = stored else {
        return Ok(Vec::new());
    };
    let stored: Vec<StoredConfirmation> =
        serde_json::from_value(value).map_err(|source| EngineError::Decode {
            what: "stored confirmation",
            source,
        })?;
    Ok(stored
        .into_iter()
        .map(StoredConfirmation::into_record)
        .collect())
}
