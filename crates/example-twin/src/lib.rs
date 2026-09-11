use br_core_integration::{
    Actor, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent,
    ServiceAccountId, UserId,
};
use chrono::Utc;
use example_contract::{
    CreateCard, PERSON_PREFIX, PersonCreated, PublishedPerson, REPLY_ACCUMULATOR, ReplyCancelled,
    ReplyFinished, SERVICE, create_card_coords, person_created_coords, reply_cancelled_coords,
    reply_finished_coords,
};
use service_engine::nats::{KvKey, Nats, NatsError, StreamFrame, command_subject, event_subject};
use uuid::Uuid;

mod read;
pub use read::*;

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
    send_create_card_with_id(nats, cmd, actor, cmd.card_id).await
}

pub async fn send_create_card_with_id(
    nats: &Nats,
    cmd: &CreateCard,
    actor: Actor,
    message_id: Uuid,
) -> Result<(), NatsError> {
    let coords = create_card_coords();
    let subject = command_subject(&coords);
    let body = serde_json::to_value(cmd).map_err(NatsError::Encode)?;
    let envelope = IntegrationCommand::new(
        message_id,
        command_type(&coords),
        coords.version,
        Utc::now(),
        metadata_as(actor, message_id),
        body,
    );
    let payload = serde_json::to_value(&envelope).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &message_id.to_string())
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
