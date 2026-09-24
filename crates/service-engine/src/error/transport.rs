use thiserror::Error;

use super::BoxedError;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransportError {
    #[error("listener connection")]
    Listen(#[source] sqlx::Error),

    #[error("staging impacts")]
    Stage(#[source] sqlx::Error),

    #[error("impact payload")]
    Payload(#[source] serde_json::Error),

    #[error("a single impact renders to {size} bytes, over the {limit}-byte NOTIFY payload limit")]
    PayloadTooLarge { size: usize, limit: usize },

    #[error("malformed impact frame: {0}")]
    Frame(String),

    #[error(
        "the impact listener was already taken; listen() is single-use so a second, unprobed \
         listener never starts"
    )]
    ListenerConsumed,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RelayError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("publishing a claimed row: {0}")]
    Publish(String),

    #[error(transparent)]
    Relay(BoxedError),
}

impl RelayError {
    pub(crate) fn published_language(error: crate::nats::NatsError) -> Self {
        Self::Publish(format!(
            "published language: {}",
            crate::chain::describe(&error)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::RelayError;
    use crate::nats::NatsError;

    #[test]
    fn a_published_language_failure_keeps_the_cause_its_nats_error_holds_as_a_source() {
        let decoding = serde_json::from_str::<u8>("").expect_err("an empty document is no u8");
        let decoding_text = decoding.to_string();
        let nats = NatsError::Decode {
            key: "offers/v1/a".to_string(),
            source: decoding,
        };

        let relayed = RelayError::published_language(nats);

        assert_eq!(
            relayed.to_string(),
            format!(
                "publishing a claimed row: published language: decoding kv key offers/v1/a: {decoding_text}"
            )
        );
    }
}
