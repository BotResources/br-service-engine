mod health;
mod key;
mod kv;

pub use health::{REASON_NO_STREAM, RelayHealth, RelayHealthChannel, RelayHealthReceiver};
pub use key::{KvKey, KvKeyError, KvPrefix};
pub use kv::{KvBucket, KvEvent, KvWatch, Revision};

use async_nats::jetstream::Context;
use async_nats::jetstream::stream::Stream;
use br_core_integration::{CommandCoords, EventCoords, IntegrationEvent};
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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum NatsError {
    #[error("connection to NATS failed: {0}")]
    Connect(String),
    #[error("stream {name} is absent; gitops declares streams, the engine only binds them")]
    NoStream { name: String },
    #[error("kv bucket {name} is absent; gitops declares buckets, the engine only binds them")]
    NoBucket { name: String },
    #[error("kv operation on {key} failed: {detail}")]
    Kv { key: String, detail: String },
    #[error("a postgres read backing a nats operation failed: {detail}")]
    Store { detail: String },
    #[error("kv key {key} was written at another revision than the {expected} expected")]
    RevisionConflict { key: String, expected: u64 },
    #[error("publish to {subject} failed ({kind}): {detail}")]
    Publish {
        subject: String,
        kind: PublishFailure,
        detail: String,
    },
    #[error("encoding a kv value failed")]
    Encode(#[source] serde_json::Error),
    #[error("decoding kv key {key}")]
    Decode {
        key: String,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublishFailure {
    NoStream,
    Transient,
}

impl std::fmt::Display for PublishFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NoStream => "no stream for subject",
            Self::Transient => "transient",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublishOutcome {
    Stored { sequence: u64 },
    Duplicate { sequence: u64 },
}

impl PublishOutcome {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::Stored { sequence } | Self::Duplicate { sequence } => *sequence,
        }
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate { .. })
    }

    fn from_ack(ack: &async_nats::jetstream::publish::PublishAck) -> Self {
        if ack.duplicate {
            Self::Duplicate {
                sequence: ack.sequence,
            }
        } else {
            Self::Stored {
                sequence: ack.sequence,
            }
        }
    }
}

#[derive(Clone)]
pub struct Nats {
    jetstream: Context,
}

impl Nats {
    pub fn from_context(jetstream: Context) -> Self {
        Self { jetstream }
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
        Ok(KvBucket::bind(store))
    }

    pub async fn published_language<V>(&self) -> Result<KvBucket<V>, NatsError> {
        self.bind_kv(KV_PUBLISHED_LANGUAGE).await
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
        let ack_future = self
            .jetstream
            .publish(subject.clone(), bytes.into())
            .await
            .map_err(|e| publish_error(&subject, e.kind(), &e))?;
        let ack = ack_future
            .await
            .map_err(|e| publish_error(&subject, e.kind(), &e))?;
        Ok(PublishOutcome::from_ack(&ack))
    }

    pub async fn publish_value_with_id(
        &self,
        subject: &str,
        payload: &serde_json::Value,
        message_id: &str,
    ) -> Result<PublishOutcome, NatsError> {
        let bytes = serde_json::to_vec(payload).map_err(NatsError::Encode)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id);
        let ack_future = self
            .jetstream
            .publish_with_headers(subject.to_string(), headers, bytes.into())
            .await
            .map_err(|e| publish_error(subject, e.kind(), &e))?;
        let ack = ack_future
            .await
            .map_err(|e| publish_error(subject, e.kind(), &e))?;
        Ok(PublishOutcome::from_ack(&ack))
    }
}

fn publish_error(
    subject: &str,
    kind: async_nats::jetstream::context::PublishErrorKind,
    detail: &dyn std::fmt::Display,
) -> NatsError {
    use async_nats::jetstream::context::PublishErrorKind as K;
    let kind = match kind {
        K::StreamNotFound => PublishFailure::NoStream,
        _ => PublishFailure::Transient,
    };
    NatsError::Publish {
        subject: subject.to_string(),
        kind,
        detail: detail.to_string(),
    }
}

impl std::fmt::Debug for Nats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nats").finish_non_exhaustive()
    }
}
