use example_contract::{
    CardReady, CreateCard, PERSON_PREFIX, PersonCreated, PublishedPerson, card_ready_coords,
    create_card_coords, person_created_coords,
};
use futures_util::StreamExt;
use service_engine::nats::{KvKey, Nats, NatsError, command_subject, event_subject};
use uuid::Uuid;

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
    let subject = command_subject(&create_card_coords());
    let payload = serde_json::to_value(cmd).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &cmd.card_id.to_string())
        .await?;
    Ok(())
}

pub async fn send_person_created(nats: &Nats, event: &PersonCreated) -> Result<(), NatsError> {
    let subject = event_subject(&person_created_coords());
    let payload = serde_json::to_value(event).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &event.person_id.to_string())
        .await?;
    Ok(())
}

#[cfg(feature = "reply")]
pub async fn send_reply_finished(
    nats: &Nats,
    reply_id: Uuid,
    board_id: Uuid,
) -> Result<(), NatsError> {
    let subject = command_subject(&crate::slices::reply::reply_finished_coords());
    let payload = serde_json::json!({ "reply_id": reply_id, "board_id": board_id });
    nats.publish_value_with_id(&subject, &payload, &reply_id.to_string())
        .await?;
    Ok(())
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
        Some(message) => Ok(serde_json::from_slice(&message.payload).ok()),
        None => Ok(None),
    }
}

pub async fn card_ready_from_stream(nats: &Nats) -> Result<Option<CardReady>, NatsError> {
    use futures_util::StreamExt;
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
            let card = serde_json::from_slice::<CardReady>(&message.payload).ok();
            let _ = message.ack().await;
            Ok(card)
        }
        _ => Ok(None),
    }
}
