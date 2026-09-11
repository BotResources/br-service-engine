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
    #[error(
        "the message carries no resolvable id: neither a {HEADER_MESSAGE_ID} header, an integration envelope id, nor a uuid Nats-Msg-Id is present"
    )]
    NoMessageId,
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
        let decoded = Decoded::from(&payload);
        let Some(body) = decoded.body else {
            return Err(Unidentified::NotEnveloped);
        };
        let message_id = message_id(headers, decoded.id).ok_or(Unidentified::NoMessageId)?;
        Ok(Self {
            reaction: reaction.to_string(),
            source,
            subject,
            message_id,
            sequence: sequence(headers),
            metadata: decoded.metadata,
            body,
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

struct Decoded {
    id: Option<Uuid>,
    metadata: MessageMetadata,
    body: Option<Bytes>,
}

impl Decoded {
    fn from(raw: &Bytes) -> Self {
        let Some(shell) = serde_json::from_slice::<EnvelopeShell>(raw).ok() else {
            return Self {
                id: None,
                metadata: MessageMetadata::default(),
                body: None,
            };
        };
        let id = shell.event_id.or(shell.command_id);
        let Some(inner) = shell.payload else {
            return Self {
                id: None,
                metadata: MessageMetadata::default(),
                body: None,
            };
        };
        if id.is_none() {
            return Self {
                id: None,
                metadata: MessageMetadata::default(),
                body: None,
            };
        }
        let metadata = shell
            .metadata
            .map(|m| MessageMetadata {
                actor: Some(m.actor),
                correlation_id: Some(m.correlation_id),
                causation_id: m.causation_id,
            })
            .unwrap_or_default();
        let body = serde_json::to_vec(&inner).ok().map(Bytes::from);
        Self { id, metadata, body }
    }
}

fn header<'a>(headers: Option<&'a HeaderMap>, name: &str) -> Option<&'a str> {
    headers.and_then(|h| h.get(name)).map(|v| v.as_str())
}

pub(crate) fn resolved_id(headers: Option<&HeaderMap>) -> Option<Uuid> {
    message_id(headers, None)
}

fn message_id(headers: Option<&HeaderMap>, envelope_id: Option<Uuid>) -> Option<Uuid> {
    header(headers, HEADER_MESSAGE_ID)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .or(envelope_id)
        .or_else(|| {
            header(headers, async_nats::header::NATS_MESSAGE_ID.as_ref())
                .and_then(|raw| Uuid::parse_str(raw).ok())
        })
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
