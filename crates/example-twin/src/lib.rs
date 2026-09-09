use example_contract::{
    CardReady, CreateCard, PERSON_PREFIX, PersonCreated, PublishedPerson, REPLY_ACCUMULATOR,
    ReplyCancelled, ReplyFinished, SERVICE, SealFailed, card_ready_coords, create_card_coords,
    person_created_coords, reply_cancelled_coords, reply_finished_coords, seal_failed_coords,
};
use futures_util::StreamExt;
use service_engine::nats::{KvKey, Nats, NatsError, StreamFrame, command_subject, event_subject};
use uuid::Uuid;

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

pub async fn send_reply_finished(nats: &Nats, finished: &ReplyFinished) -> Result<(), NatsError> {
    let subject = command_subject(&reply_finished_coords());
    let payload = serde_json::to_value(finished).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &finished.reply_id.to_string())
        .await?;
    Ok(())
}

pub async fn resend_reply_finished(nats: &Nats, finished: &ReplyFinished) -> Result<(), NatsError> {
    let subject = command_subject(&reply_finished_coords());
    let payload = serde_json::to_value(finished).map_err(NatsError::Encode)?;
    nats.publish_value_with_id(&subject, &payload, &Uuid::now_v7().to_string())
        .await?;
    Ok(())
}

pub async fn send_reply_cancelled(
    nats: &Nats,
    cancelled: &ReplyCancelled,
) -> Result<(), NatsError> {
    let subject = command_subject(&reply_cancelled_coords());
    let payload = serde_json::to_value(cancelled).map_err(NatsError::Encode)?;
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
            let failed = serde_json::from_slice::<SealFailed>(&message.payload).ok();
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
        Some(message) => Ok(serde_json::from_slice(&message.payload).ok()),
        None => Ok(None),
    }
}

pub async fn card_ready_from_stream(nats: &Nats) -> Result<Option<CardReady>, NatsError> {
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
