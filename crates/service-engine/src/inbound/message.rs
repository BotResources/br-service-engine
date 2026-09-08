use async_nats::HeaderMap;
use bytes::Bytes;
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

#[derive(Debug, Clone, thiserror::Error)]
pub enum Unidentified {
    #[error(
        "the message carries no resolvable id: neither a {HEADER_MESSAGE_ID} header nor a Nats-Msg-Id is present or parses as a uuid"
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
        let message_id = message_id(headers).ok_or(Unidentified::NoMessageId)?;
        Ok(Self {
            reaction: reaction.to_string(),
            source,
            subject,
            message_id,
            sequence: sequence(headers),
            payload,
            delivered,
        })
    }
}

fn header<'a>(headers: Option<&'a HeaderMap>, name: &str) -> Option<&'a str> {
    headers.and_then(|h| h.get(name)).map(|v| v.as_str())
}

fn message_id(headers: Option<&HeaderMap>) -> Option<Uuid> {
    let raw = header(headers, HEADER_MESSAGE_ID)
        .or_else(|| header(headers, async_nats::header::NATS_MESSAGE_ID.as_ref()))?;
    Uuid::parse_str(raw).ok()
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

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, *value);
        }
        map
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
