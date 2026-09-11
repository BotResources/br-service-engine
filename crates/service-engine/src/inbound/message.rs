use async_nats::HeaderMap;
use br_core_integration::{Actor, EventMetadata};
use bytes::Bytes;
use serde::Deserialize;
use uuid::Uuid;

pub const HEADER_MESSAGE_ID: &str = "Br-Message-Id";
pub const HEADER_SEQ: &str = "Br-Seq";
pub const HEADER_SEQ_KEY: &str = "Br-Seq-Key";
pub const HEADER_PRODUCER: &str = "Br-Producer";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Command,
    Event,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceKey {
    pub producer: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sequenced {
    pub key: SequenceKey,
    pub seq: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageMetadata {
    pub actor: Option<Actor>,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Unidentified {
    #[error(
        "the frame carries no br-core-integration envelope: the frontier accepts only an integration command or event, so a bare body is refused as terminal"
    )]
    NotEnveloped,
}

#[derive(Debug, Clone)]
pub struct Incoming {
    pub reaction: String,
    pub source: Source,
    pub subject: String,
    pub message_id: Uuid,
    pub sequence: Option<Sequenced>,
    pub metadata: MessageMetadata,
    pub body: Bytes,
    pub payload: Bytes,
    pub delivered: u32,
}

impl Incoming {
    pub fn identify(
        reaction: &str,
        source: Source,
        subject: String,
        headers: Option<&HeaderMap>,
        payload: Bytes,
        delivered: u32,
    ) -> Result<Self, Unidentified> {
        let Some(decoded) = Enveloped::decode(&payload) else {
            return Err(Unidentified::NotEnveloped);
        };
        Ok(Self {
            reaction: reaction.to_string(),
            source,
            subject,
            message_id: message_id(headers, decoded.id),
            sequence: sequence(headers),
            metadata: decoded.metadata,
            body: decoded.body,
            payload,
            delivered,
        })
    }
}

#[derive(Deserialize)]
struct EnvelopeShell {
    #[serde(default)]
    event_id: Option<Uuid>,
    #[serde(default)]
    command_id: Option<Uuid>,
    #[serde(default)]
    metadata: Option<EventMetadata>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

struct Enveloped {
    id: Uuid,
    metadata: MessageMetadata,
    body: Bytes,
}

impl Enveloped {
    fn decode(raw: &Bytes) -> Option<Self> {
        let shell = serde_json::from_slice::<EnvelopeShell>(raw).ok()?;
        let id = shell.event_id.or(shell.command_id)?;
        let inner = shell.payload?;
        let metadata = shell
            .metadata
            .map(|m| MessageMetadata {
                actor: Some(m.actor),
                correlation_id: Some(m.correlation_id),
                causation_id: m.causation_id,
            })
            .unwrap_or_default();
        let body = Bytes::from(serde_json::to_vec(&inner).ok()?);
        Some(Self { id, metadata, body })
    }
}

fn header<'a>(headers: Option<&'a HeaderMap>, name: &str) -> Option<&'a str> {
    headers.and_then(|h| h.get(name)).map(|v| v.as_str())
}

fn header_id(headers: Option<&HeaderMap>, name: &str) -> Option<Uuid> {
    header(headers, name).and_then(|raw| Uuid::parse_str(raw).ok())
}

pub(crate) fn resolved_id(headers: Option<&HeaderMap>) -> Option<Uuid> {
    header_id(headers, HEADER_MESSAGE_ID)
        .or_else(|| header_id(headers, async_nats::header::NATS_MESSAGE_ID.as_ref()))
}

fn message_id(headers: Option<&HeaderMap>, envelope_id: Uuid) -> Uuid {
    header_id(headers, HEADER_MESSAGE_ID).unwrap_or(envelope_id)
}

fn sequence(headers: Option<&HeaderMap>) -> Option<Sequenced> {
    let seq = header(headers, HEADER_SEQ)?.parse::<u64>().ok()?;
    let key = header(headers, HEADER_SEQ_KEY)?.to_string();
    let producer = header(headers, HEADER_PRODUCER)?.to_string();
    Some(Sequenced {
        key: SequenceKey { producer, key },
        seq,
    })
}

#[cfg(test)]
#[path = "message_tests.rs"]
mod tests;
