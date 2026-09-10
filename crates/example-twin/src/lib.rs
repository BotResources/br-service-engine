use br_core_integration::{
    Actor, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent,
    ServiceAccountId, UserId,
};
use chrono::Utc;
use example_contract::{
    CardReady, CreateCard, PERSON_PREFIX, PersonCreated, PublishedPerson, REPLY_ACCUMULATOR,
    ReplyCancelled, ReplyFinished, SERVICE, SealFailed, card_ready_coords, create_card_coords,
    person_created_coords, reply_cancelled_coords, reply_finished_coords, seal_failed_coords,
};
use futures_util::StreamExt;
use service_engine::nats::{KvKey, Nats, NatsError, StreamFrame, command_subject, event_subject};
use uuid::Uuid;

const TWIN_SERVICE_ID: Uuid = Uuid::from_u128(0x7217_7a11_4b27_4d59_9e10_a3f2_77c5_8d41);

pub fn twin_actor() -> Actor {
    Actor::Service(ServiceAccountId::from(TWIN_SERVICE_ID))
}

pub fn human_actor() -> Actor {
    Actor::Human(UserId::from(Uuid::now_v7()))
}

fn metadata(correlation: Uuid) -> EventMetadata {
    metadata_as(twin_actor(), correlation)
}

fn metadata_as(actor: Actor, correlation: Uuid) -> EventMetadata {
    EventMetadata::new(actor, correlation)
}

fn command_type(coords: &CommandCoords) -> String {
    format!("{}.{}", coords.aggregate.as_str(), coords.verb.as_str())
}

fn event_type(coords: &EventCoords) -> String {
    format!("{}.{}", coords.aggregate.as_str(), coords.fact.as_str())
}

fn command_envelope(
    coords: &CommandCoords,
    id: Uuid,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NatsError> {
    let envelope = IntegrationCommand::new(
        id,
        command_type(coords),
        coords.version,
        Utc::now(),
        metadata(id),
        payload,
    );
    serde_json::to_value(&envelope).map_err(NatsError::Encode)
}

fn event_envelope(
    coords: &EventCoords,
    id: Uuid,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NatsError> {
    let envelope = IntegrationEvent::new(
        id,
        event_type(coords),
        coords.version,
        Utc::now(),
        metadata(id),
        payload,
    );
    serde_json::to_value(&envelope).map_err(NatsError::Encode)
}

pub async fn stream_reply_chunk(
    nats: &Nats,
    reply_id: Uuid,
    seq: u64,
    chunk: &str,
) -> Result<(), NatsError> {
    let frame = StreamFrame {
        accumulator: REPLY_ACCUMULATOR.to_string(),
        key: serde_json::json!(reply_id),
        seq,
        chunk: serde_json::json!(chunk),
    };
    nats.publish_chunk(SERVICE, &frame).await?;
    Ok(())
}

pub async fn stream_reply(nats: &Nats, reply_id: Uuid, chunks: &[&str]) -> Result<(), NatsError> {
    for (seq, chunk) in chunks.iter().enumerate() {
        stream_reply_chunk(nats, reply_id, seq as u64, chunk).await?;
    }
    Ok(())
}

pub fn person_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{PERSON_PREFIX}{id}")).expect("a published-person key is valid")
}

pub async fn publish_person(nats: &Nats, person: &PublishedPerson) -> Result<(), NatsError> {
    let bucket = nats.published_language::<PublishedPerson>().await?;
    bucket.put(&person_key(person.id), person).await?;
    Ok(())
}

pub async fn retract_person(nats: &Nats, id: Uuid) -> Result<(), NatsError> {
    let bucket = nats.published_language::<PublishedPerson>().await?;
    bucket.retract(&person_key(id)).await?;
    Ok(())
}

pub async fn send_create_card(nats: &Nats, cmd: &CreateCard) -> Result<(), NatsError> {
    send_create_card_as(nats, cmd, twin_actor()).await
}

pub async fn send_create_card_as(
    nats: &Nats,
    cmd: &CreateCard,
    actor: Actor,
) -> Result<(), NatsError> {
    let coords = create_card_coords();
    let subject = command_subject(&coords);
    let body = serde_json::to_value(cmd).map_err(NatsError::Encode)?;
    let envelope = IntegrationCommand::new(
        cmd.card_id,
        command_type(&coords),
        coords.version,
        Utc::now(),
        metadata_as(actor, cmd.card_id),
        body,
    );
    let payload = serde_json::to_value(&envelope).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &cmd.card_id.to_string())
        .await?;
    Ok(())
}

pub async fn send_person_created(nats: &Nats, event: &PersonCreated) -> Result<(), NatsError> {
    let coords = person_created_coords();
    let subject = event_subject(&coords);
    let body = serde_json::to_value(event).map_err(NatsError::Encode)?;
    let payload = event_envelope(&coords, event.person_id, body)?;
    nats.publish_value_with_id(&subject, &payload, &event.person_id.to_string())
        .await?;
    Ok(())
}

pub async fn send_person_created_seq(
    nats: &Nats,
    event: &PersonCreated,
    seq: u64,
) -> Result<(), NatsError> {
    let coords = person_created_coords();
    let subject = event_subject(&coords);
    let id = Uuid::now_v7();
    let body = serde_json::to_value(event).map_err(NatsError::Encode)?;
    let payload = event_envelope(&coords, id, body)?;
    nats.publish_value_sequenced(
        &subject,
        &payload,
        &id.to_string(),
        Some(("twin", &event.person_id.to_string(), seq as i64)),
    )
    .await?;
    Ok(())
}

pub async fn send_reply_finished(nats: &Nats, finished: &ReplyFinished) -> Result<(), NatsError> {
    let coords = reply_finished_coords();
    let subject = command_subject(&coords);
    let body = serde_json::to_value(finished).map_err(NatsError::Encode)?;
    let payload = command_envelope(&coords, finished.reply_id, body)?;
    nats.publish_value_with_id(&subject, &payload, &finished.reply_id.to_string())
        .await?;
    Ok(())
}

pub async fn send_reply_cancelled(
    nats: &Nats,
    cancelled: &ReplyCancelled,
) -> Result<(), NatsError> {
    let coords = reply_cancelled_coords();
    let subject = command_subject(&coords);
    let body = serde_json::to_value(cancelled).map_err(NatsError::Encode)?;
    let payload = command_envelope(&coords, cancelled.reply_id, body)?;
    nats.publish_value_with_id(&subject, &payload, &cancelled.reply_id.to_string())
        .await?;
    Ok(())
}

pub async fn seal_failed_from_stream(nats: &Nats) -> Result<Option<SealFailed>, NatsError> {
    let subject = event_subject(&seal_failed_coords());
    let stream = nats
        .context()
        .get_stream("INTEGRATION_EVT")
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            filter_subject: subject,
            ..Default::default()
        })
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    match batch.next().await {
        Some(Ok(message)) => {
            let failed = serde_json::from_slice::<IntegrationEvent<SealFailed>>(&message.payload)
                .ok()
                .map(|envelope| envelope.payload);
            let _ = message.ack().await;
            Ok(failed)
        }
        _ => Ok(None),
    }
}

pub async fn next_card_ready(nats: &Nats) -> Result<Option<CardReady>, NatsError> {
    let subject = event_subject(&card_ready_coords());
    let mut subscriber = nats
        .context()
        .client()
        .subscribe(subject)
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    match subscriber.next().await {
        Some(message) => Ok(decode_card_ready(&message.payload)),
        None => Ok(None),
    }
}

pub struct CardReadyEnvelope {
    pub envelope: IntegrationEvent<CardReady>,
    pub has_producer_sequence: bool,
}

pub async fn card_ready_from_stream(nats: &Nats) -> Result<Option<CardReady>, NatsError> {
    Ok(card_ready_envelope_from_stream(nats)
        .await?
        .map(|read| read.envelope.payload))
}

pub async fn card_ready_envelope_from_stream(
    nats: &Nats,
) -> Result<Option<CardReadyEnvelope>, NatsError> {
    let subject = event_subject(&card_ready_coords());
    let stream = nats
        .context()
        .get_stream("INTEGRATION_EVT")
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            filter_subject: subject,
            ..Default::default()
        })
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .map_err(|error| NatsError::Connect(error.to_string()))?;
    match batch.next().await {
        Some(Ok(message)) => {
            let read = serde_json::from_slice::<IntegrationEvent<CardReady>>(&message.payload)
                .ok()
                .map(|envelope| CardReadyEnvelope {
                    envelope,
                    has_producer_sequence: carries_producer_sequence(message.headers.as_ref()),
                });
            let _ = message.ack().await;
            Ok(read)
        }
        _ => Ok(None),
    }
}

fn carries_producer_sequence(headers: Option<&async_nats::HeaderMap>) -> bool {
    let Some(headers) = headers else {
        return false;
    };
    headers.get("Br-Producer").is_some()
        && headers.get("Br-Seq-Key").is_some()
        && headers.get("Br-Seq").is_some()
}

fn decode_card_ready(bytes: &[u8]) -> Option<CardReady> {
    serde_json::from_slice::<IntegrationEvent<CardReady>>(bytes)
        .ok()
        .map(|envelope| envelope.payload)
}
