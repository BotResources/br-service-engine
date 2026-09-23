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
