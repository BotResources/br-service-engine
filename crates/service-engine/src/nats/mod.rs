mod grace;
mod health;
mod key;
mod kv;
mod publish;
mod streaming;

pub use grace::{NatsCondition, NatsHealth, NatsHealthChannel, NatsHealthReceiver};
pub use health::{REASON_NO_STREAM, RelayHealth, RelayHealthChannel, RelayHealthReceiver};
pub use key::{KvKey, KvKeyError, KvPrefix};
pub use kv::{KvBucket, KvEvent, KvWatch, Revision, Watched};
pub use publish::{NatsError, PublishFailure, PublishOutcome};
pub use streaming::{
    StreamFrame, chunk_subject, streaming_filter, streaming_stream, subject_token,
};

use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::jetstream::stream::Stream;
use br_core_integration::{CommandCoords, EventCoords, IntegrationEvent};
use publish::publish_error;
use serde::Serialize;

pub fn event_subject(coords: &EventCoords) -> String {
    format!(
        "integration.evt.{}.{}.{}.v{}",
        coords.producer.as_str(),
        coords.aggregate.as_str(),
        coords.fact.as_str(),
        coords.version
    )
}

pub fn command_subject(coords: &CommandCoords) -> String {
    format!(
        "integration.cmd.{}.{}.{}.v{}",
        coords.receiver.as_str(),
        coords.aggregate.as_str(),
        coords.verb.as_str(),
        coords.version
    )
}

pub const INTEGRATION_CMD: &str = "INTEGRATION_CMD";
pub const INTEGRATION_EVT: &str = "INTEGRATION_EVT";
pub const KV_PUBLISHED_LANGUAGE: &str = "PUBLISHED_LANGUAGE";

pub const PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Nats {
    jetstream: Context,
    publish_ack_timeout: Duration,
}

impl Nats {
    pub fn from_context(jetstream: Context) -> Self {
        Self {
            jetstream,
            publish_ack_timeout: PUBLISH_ACK_TIMEOUT,
        }
    }

    pub fn with_publish_ack_timeout(mut self, publish_ack_timeout: Duration) -> Self {
        self.publish_ack_timeout = publish_ack_timeout;
        self
    }

    pub async fn connect(url: &str) -> Result<Self, NatsError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| NatsError::Connect(e.to_string()))?;
        Ok(Self::from_context(async_nats::jetstream::new(client)))
    }

    pub fn context(&self) -> &Context {
        &self.jetstream
    }

    pub fn connection_state(&self) -> async_nats::connection::State {
        self.jetstream.client().connection_state()
    }

    pub fn reachable(&self) -> bool {
        self.connection_state() == async_nats::connection::State::Connected
    }

    pub async fn bind_stream(&self, name: &str) -> Result<Stream, NatsError> {
        self.jetstream
            .get_stream(name)
            .await
            .map_err(|_| NatsError::NoStream {
                name: name.to_string(),
            })
    }

    pub async fn bind_kv<V>(&self, bucket: &str) -> Result<KvBucket<V>, NatsError> {
        let store =
            self.jetstream
                .get_key_value(bucket)
                .await
                .map_err(|_| NatsError::NoBucket {
                    name: bucket.to_string(),
                })?;
        Ok(KvBucket::bind(store, self.jetstream.clone()))
    }

    pub async fn published_language<V>(&self) -> Result<KvBucket<V>, NatsError> {
        self.bind_kv(KV_PUBLISHED_LANGUAGE).await
    }

    pub async fn bind_ephemeral<V>(&self, name: &str) -> Result<KvBucket<V>, NatsError> {
        let bucket = self.bind_kv::<V>(name).await?;
        if bucket.max_age().await?.is_zero() {
            return Err(NatsError::EphemeralNotConfigured {
                name: name.to_string(),
                detail: "no TTL: max_age is zero, so a presence key would never expire",
            });
        }
        let backing = self.bind_stream(&format!("KV_{name}")).await?;
        let config = &backing.cached_info().config;
        if config.subject_delete_marker_ttl.is_none() {
            return Err(NatsError::EphemeralNotConfigured {
                name: name.to_string(),
                detail: "no delete markers: an expired key would raise no watch event, so a \
                         presence Remove could never fire",
            });
        }
        if !config.allow_message_ttl {
            return Err(NatsError::EphemeralNotConfigured {
                name: name.to_string(),
                detail: "no per-message TTL: the bucket refuses a Nats-TTL header, so a lane's \
                         declared lifetime could not be applied per key",
            });
        }
        Ok(bucket)
    }

    pub async fn ping(&self) -> Result<(), NatsError> {
        self.jetstream
            .client()
            .flush()
            .await
            .map_err(|e| NatsError::Connect(e.to_string()))
    }

    pub async fn publish_event<T: Serialize>(
        &self,
        coords: &EventCoords,
        event: &IntegrationEvent<T>,
    ) -> Result<PublishOutcome, NatsError> {
        let subject = event_subject(coords);
        let bytes = serde_json::to_vec(event).map_err(NatsError::Encode)?;
        let publish = async {
            let ack_future = self
                .jetstream
                .publish(subject.clone(), bytes.into())
                .await
                .map_err(|e| publish_error(&subject, e.kind(), &e))?;
            let ack = ack_future
                .await
                .map_err(|e| publish_error(&subject, e.kind(), &e))?;
            Ok(PublishOutcome::from_ack(&ack))
        };
        match tokio::time::timeout(self.publish_ack_timeout, publish).await {
            Ok(result) => result,
            Err(_elapsed) => Err(self.ack_timeout(subject)),
        }
    }

    fn ack_timeout(&self, subject: String) -> NatsError {
        NatsError::Publish {
            subject,
            kind: PublishFailure::Unanswered,
            detail: format!(
                "publish ack did not return within {:?}; the broker is unreachable or wedged",
                self.publish_ack_timeout
            ),
        }
    }

    pub async fn publish_value_with_id(
        &self,
        subject: &str,
        payload: &serde_json::Value,
        message_id: &str,
    ) -> Result<PublishOutcome, NatsError> {
        self.publish_value_sequenced(subject, payload, message_id, None)
            .await
    }

    pub async fn publish_value_sequenced(
        &self,
        subject: &str,
        payload: &serde_json::Value,
        message_id: &str,
        sequence: Option<(&str, &str, i64)>,
    ) -> Result<PublishOutcome, NatsError> {
        let bytes = serde_json::to_vec(payload).map_err(NatsError::Encode)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id);
        headers.insert(crate::inbound::HEADER_MESSAGE_ID, message_id);
        if let Some((producer, seq_key, seq)) = sequence {
            headers.insert(crate::inbound::HEADER_PRODUCER, producer);
            headers.insert(crate::inbound::HEADER_SEQ_KEY, seq_key);
            headers.insert(crate::inbound::HEADER_SEQ, seq.to_string().as_str());
        }
        self.publish_encoded(subject, headers, bytes).await
    }

    pub async fn publish_bytes_with_id(
        &self,
        subject: &str,
        payload: &[u8],
        message_id: &str,
    ) -> Result<PublishOutcome, NatsError> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id);
        headers.insert(crate::inbound::HEADER_MESSAGE_ID, message_id);
        self.publish_encoded(subject, headers, payload.to_vec())
            .await
    }

    async fn publish_encoded(
        &self,
        subject: &str,
        headers: async_nats::HeaderMap,
        bytes: Vec<u8>,
    ) -> Result<PublishOutcome, NatsError> {
        let publish = async {
            let ack_future = self
                .jetstream
                .publish_with_headers(subject.to_string(), headers, bytes.into())
                .await
                .map_err(|e| publish_error(subject, e.kind(), &e))?;
            let ack = ack_future
                .await
                .map_err(|e| publish_error(subject, e.kind(), &e))?;
            Ok(PublishOutcome::from_ack(&ack))
        };
        match tokio::time::timeout(self.publish_ack_timeout, publish).await {
            Ok(result) => result,
            Err(_elapsed) => Err(self.ack_timeout(subject.to_string())),
        }
    }

    pub async fn stream_max_age(&self, name: &str) -> Result<Duration, NatsError> {
        let stream = self.bind_stream(name).await?;
        Ok(stream.cached_info().config.max_age)
    }

    pub async fn purge_chunk_subject(&self, stream: &str, subject: &str) -> Result<(), NatsError> {
        let stream = self.bind_stream(stream).await?;
        stream
            .purge()
            .filter(subject)
            .await
            .map_err(|error| NatsError::Store {
                detail: format!(
                    "purging the sealed subject {subject}: {}",
                    crate::chain::describe(&error)
                ),
            })?;
        Ok(())
    }

    pub async fn publish_chunk(
        &self,
        service: &str,
        frame: &StreamFrame,
    ) -> Result<PublishOutcome, NatsError> {
        let token = subject_token(&frame.key).map_err(|error| NatsError::Store {
            detail: crate::chain::describe(&error),
        })?;
        let subject = chunk_subject(service, &token);
        let message_id = frame.message_id(&token);
        let payload = serde_json::to_value(frame).map_err(NatsError::Encode)?;
        self.publish_value_with_id(&subject, &payload, &message_id)
            .await
    }
}

impl std::fmt::Debug for Nats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nats").finish_non_exhaustive()
    }
}
