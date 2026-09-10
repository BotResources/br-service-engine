use async_nats::HeaderMap;
use bytes::Bytes;
use uuid::Uuid;

use crate::inbound::deadletter::DeadLetter;
use crate::inbound::message::{HEADER_MESSAGE_ID, HEADER_PRODUCER, HEADER_SEQ, HEADER_SEQ_KEY};
use crate::nats::{Nats, NatsError};

pub(crate) async fn republish(nats: &Nats, entry: &DeadLetter) -> Result<(), NatsError> {
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
pub(crate) struct Row {
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
    pub(crate) fn into_dead_letter(self) -> DeadLetter {
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
