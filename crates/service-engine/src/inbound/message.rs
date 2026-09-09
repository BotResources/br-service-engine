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
        let message_id = message_id(headers, decoded.id).ok_or(Unidentified::NoMessageId)?;
        let body = decoded.body.unwrap_or_else(|| payload.clone());
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
mod tests {
    use super::*;
    use br_core_integration::{IntegrationCommand, ServiceAccountId};
    use chrono::Utc;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, *value);
        }
        map
    }

    fn enveloped(command_id: Uuid, actor: Actor, correlation_id: Uuid) -> Bytes {
        let envelope = IntegrationCommand::new(
            command_id,
            "work.do",
            1,
            Utc::now(),
            EventMetadata::new(actor, correlation_id).with_causation(correlation_id),
            serde_json::json!({ "verdict": "ok" }),
        );
        Bytes::from(serde_json::to_vec(&envelope).unwrap())
    }

    #[test]
    fn a_message_id_header_is_preferred_over_the_nats_dedup_token() {
        let id = Uuid::now_v7();
        let dedup = Uuid::now_v7();
        let map = headers(&[
            (HEADER_MESSAGE_ID, &id.to_string()),
            (
                async_nats::header::NATS_MESSAGE_ID.as_ref(),
                &dedup.to_string(),
            ),
        ]);
        let msg = Incoming::identify(
            "r",
            Source::Command,
            "s".into(),
            Some(&map),
            Bytes::new(),
            1,
        )
        .unwrap();
        assert_eq!(msg.message_id, id);
    }

    #[test]
    fn the_nats_dedup_token_identifies_a_message_with_no_logical_id_header() {
        let dedup = Uuid::now_v7();
        let map = headers(&[(
            async_nats::header::NATS_MESSAGE_ID.as_ref(),
            &dedup.to_string(),
        )]);
        let msg = Incoming::identify("r", Source::Event, "s".into(), Some(&map), Bytes::new(), 1)
            .unwrap();
        assert_eq!(msg.message_id, dedup);
    }

    #[test]
    fn a_message_with_no_resolvable_id_is_refused() {
        let err =
            Incoming::identify("r", Source::Event, "s".into(), None, Bytes::new(), 1).unwrap_err();
        assert!(matches!(err, Unidentified::NoMessageId));
    }

    #[test]
    fn an_envelope_id_identifies_a_message_carrying_no_id_header() {
        let command_id = Uuid::now_v7();
        let actor = Actor::Service(ServiceAccountId::from(Uuid::now_v7()));
        let correlation = Uuid::now_v7();
        let raw = enveloped(command_id, actor, correlation);
        let msg = Incoming::identify("r", Source::Command, "s".into(), None, raw, 1).unwrap();
        assert_eq!(msg.message_id, command_id);
        assert_eq!(msg.metadata.actor, Some(actor));
        assert_eq!(msg.metadata.correlation_id, Some(correlation));
        assert_eq!(msg.metadata.causation_id, Some(correlation));
        assert_eq!(&msg.body[..], br#"{"verdict":"ok"}"#);
    }

    #[test]
    fn a_non_uuid_nats_msg_id_does_not_dead_letter_an_enveloped_message() {
        let command_id = Uuid::now_v7();
        let actor = Actor::Service(ServiceAccountId::from(Uuid::now_v7()));
        let raw = enveloped(command_id, actor, Uuid::now_v7());
        let map = headers(&[(async_nats::header::NATS_MESSAGE_ID.as_ref(), "not-a-uuid")]);
        let msg = Incoming::identify("r", Source::Command, "s".into(), Some(&map), raw, 1).unwrap();
        assert_eq!(msg.message_id, command_id);
    }

    #[test]
    fn a_bare_payload_keeps_its_bytes_as_the_body_and_carries_no_metadata() {
        let id = Uuid::now_v7();
        let map = headers(&[(HEADER_MESSAGE_ID, &id.to_string())]);
        let raw = Bytes::from_static(br#"{"verdict":"ok"}"#);
        let msg = Incoming::identify("r", Source::Command, "s".into(), Some(&map), raw.clone(), 1)
            .unwrap();
        assert_eq!(msg.body, raw);
        assert_eq!(msg.metadata, MessageMetadata::default());
    }

    #[test]
    fn a_sequence_needs_all_three_of_its_headers() {
        let id = Uuid::now_v7();
        let complete = headers(&[
            (HEADER_MESSAGE_ID, &id.to_string()),
            (HEADER_SEQ, "7"),
            (HEADER_SEQ_KEY, "thread-1"),
            (HEADER_PRODUCER, "runner"),
        ]);
        let msg = Incoming::identify(
            "r",
            Source::Command,
            "s".into(),
            Some(&complete),
            Bytes::new(),
            1,
        )
        .unwrap();
        assert_eq!(
            msg.sequence,
            Some(Sequenced {
                key: SequenceKey {
                    producer: "runner".into(),
                    key: "thread-1".into()
                },
                seq: 7
            })
        );

        let partial = headers(&[(HEADER_MESSAGE_ID, &id.to_string()), (HEADER_SEQ, "7")]);
        let msg = Incoming::identify(
            "r",
            Source::Command,
            "s".into(),
            Some(&partial),
            Bytes::new(),
            1,
        )
        .unwrap();
        assert_eq!(msg.sequence, None);
    }
}
