#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum NatsError {
    #[error("connection to NATS failed: {0}")]
    Connect(String),
    #[error("stream {name} is absent; gitops declares streams, the engine only binds them")]
    NoStream { name: String },
    #[error("kv bucket {name} is absent; gitops declares buckets, the engine only binds them")]
    NoBucket { name: String },
    #[error("kv bucket {name} is not usable for presence: {detail}")]
    EphemeralNotConfigured { name: String, detail: &'static str },
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

    pub(crate) fn from_ack(ack: &async_nats::jetstream::publish::PublishAck) -> Self {
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

pub(crate) fn publish_error(
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
