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
    let msg =
        Incoming::identify("r", Source::Event, "s".into(), Some(&map), Bytes::new(), 1).unwrap();
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
    let msg =
        Incoming::identify("r", Source::Command, "s".into(), Some(&map), raw.clone(), 1).unwrap();
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
