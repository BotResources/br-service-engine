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
fn a_message_id_header_is_preferred_over_the_envelope_id_and_the_nats_dedup_token() {
    let header_id = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let dedup = Uuid::now_v7();
    let actor = Actor::Service(ServiceAccountId::from(Uuid::now_v7()));
    let raw = enveloped(command_id, actor, Uuid::now_v7());
    let map = headers(&[
        (HEADER_MESSAGE_ID, &header_id.to_string()),
        (
            async_nats::header::NATS_MESSAGE_ID.as_ref(),
            &dedup.to_string(),
        ),
    ]);
    let msg =
        Incoming::identify("r", Source::Command, "s".into(), Some(&map), raw, 1).unwrap();
    assert_eq!(msg.message_id, header_id);
}

#[test]
fn a_bare_frame_with_no_envelope_is_refused_as_terminal() {
    let dedup = Uuid::now_v7();
    let map = headers(&[(
        async_nats::header::NATS_MESSAGE_ID.as_ref(),
        &dedup.to_string(),
    )]);
    let err = Incoming::identify(
        "r",
        Source::Event,
        "s".into(),
        Some(&map),
        Bytes::from_static(br#"{"verdict":"ok"}"#),
        1,
    )
    .unwrap_err();
    assert!(
        matches!(err, Unidentified::NotEnveloped),
        "a frame that carries no br-core-integration envelope is refused at identify, even when a \
         dedup token would give it an id"
    );
}

#[test]
fn a_frame_with_no_envelope_and_no_headers_is_refused() {
    let err =
        Incoming::identify("r", Source::Event, "s".into(), None, Bytes::new(), 1).unwrap_err();
    assert!(matches!(err, Unidentified::NotEnveloped));
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
fn a_bare_json_payload_is_refused_because_it_carries_no_envelope() {
    let id = Uuid::now_v7();
    let map = headers(&[(HEADER_MESSAGE_ID, &id.to_string())]);
    let raw = Bytes::from_static(br#"{"verdict":"ok"}"#);
    let err =
        Incoming::identify("r", Source::Command, "s".into(), Some(&map), raw, 1).unwrap_err();
    assert!(
        matches!(err, Unidentified::NotEnveloped),
        "a plain JSON body with a message-id header is still not an integration envelope, so the \
         frontier refuses it as terminal rather than flowing it with no sender",
    );
}

#[test]
fn a_sequence_needs_all_three_of_its_headers() {
    let command_id = Uuid::now_v7();
    let actor = Actor::Service(ServiceAccountId::from(Uuid::now_v7()));
    let raw = enveloped(command_id, actor, Uuid::now_v7());
    let complete = headers(&[
        (HEADER_SEQ, "7"),
        (HEADER_SEQ_KEY, "thread-1"),
        (HEADER_PRODUCER, "runner"),
    ]);
    let msg = Incoming::identify(
        "r",
        Source::Command,
        "s".into(),
        Some(&complete),
        raw.clone(),
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

    let partial = headers(&[(HEADER_SEQ, "7")]);
    let msg = Incoming::identify(
        "r",
        Source::Command,
        "s".into(),
        Some(&partial),
        raw,
        1,
    )
    .unwrap();
    assert_eq!(msg.sequence, None);
}
