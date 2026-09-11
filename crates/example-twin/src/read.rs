use br_core_integration::IntegrationEvent;
use example_contract::{CardReady, SealFailed, card_ready_coords, seal_failed_coords};
use futures_util::StreamExt;
use service_engine::nats::{Nats, NatsError, event_subject};

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
