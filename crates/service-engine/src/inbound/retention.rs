use std::collections::BTreeSet;
use std::time::Duration;

use crate::error::EngineError;
use crate::inbound::subscription::Subscription;
use crate::nats::Nats;

pub(crate) async fn validate_message_retention(
    nats: &Nats,
    subscriptions: &[Subscription],
    message_retention: Duration,
) -> Result<(), EngineError> {
    let streams: BTreeSet<&'static str> = subscriptions
        .iter()
        .map(|subscription| subscription.coordinates.stream())
        .collect();
    for stream in streams {
        let max_age = nats.stream_max_age(stream).await?;
        if max_age.is_zero() || message_retention < max_age {
            return Err(EngineError::Config(format!(
                "message_retention {message_retention:?} does not cover the {stream} stream's \
                 max_age {max_age:?}; a durable replays from the start of the stream, so a claim \
                 swept before its message can still be redelivered would re-run the effect. Set \
                 EngineConfig::with_message_retention to at least the stream's max_age (an \
                 unlimited max_age can never be covered and is refused)"
            )));
        }
    }
    Ok(())
}
