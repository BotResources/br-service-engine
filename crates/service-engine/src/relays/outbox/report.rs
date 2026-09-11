use uuid::Uuid;

use crate::nats::{NatsError, PublishFailure};

pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_MAX_MESSAGES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RelayPolicy {
    pub max_attempts: u32,
    pub max_messages: usize,
}

impl Default for RelayPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_messages: DEFAULT_MAX_MESSAGES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RelayPass {
    pub picked: usize,
    pub published: usize,
    pub duplicates: usize,
    pub row_id_fallbacks: usize,
    pub failed: usize,
    pub retried: usize,
    pub structural: usize,
    pub dead_lettered: usize,
    pub min_retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Structural,
    Unanswered,
    Rejected,
}

pub fn classify_failure(err: &NatsError) -> FailureClass {
    match err {
        NatsError::Publish {
            kind: PublishFailure::NoStream,
            ..
        } => FailureClass::Structural,
        NatsError::Publish {
            kind: PublishFailure::Unanswered,
            ..
        } => FailureClass::Unanswered,
        _ => FailureClass::Rejected,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MessageIdSource {
    Envelope,
    Row,
}

pub(super) fn message_id_for(row_id: Uuid, payload: &serde_json::Value) -> (Uuid, MessageIdSource) {
    let envelope_id = payload
        .get("event_id")
        .or_else(|| payload.get("command_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok());
    match envelope_id {
        Some(id) => (id, MessageIdSource::Envelope),
        None => (row_id, MessageIdSource::Row),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::PublishOutcome;

    fn no_stream() -> Result<PublishOutcome, NatsError> {
        Err(NatsError::Publish {
            subject: "integration.evt.sample.assignment.relayed.v1".into(),
            kind: PublishFailure::NoStream,
            detail: "no stream".into(),
        })
    }

    fn unanswered() -> Result<PublishOutcome, NatsError> {
        Err(NatsError::Publish {
            subject: "integration.evt.sample.assignment.relayed.v1".into(),
            kind: PublishFailure::Unanswered,
            detail: "publish ack did not return".into(),
        })
    }

    fn rejected() -> Result<PublishOutcome, NatsError> {
        Err(NatsError::Publish {
            subject: "integration.evt.sample.assignment.relayed.v1".into(),
            kind: PublishFailure::Transient,
            detail: "message size exceeds the stream limit".into(),
        })
    }

    #[test]
    fn no_stream_is_structural() {
        assert_eq!(
            classify_failure(&no_stream().unwrap_err()),
            FailureClass::Structural
        );
    }

    #[test]
    fn an_unanswered_publish_is_not_an_attempt() {
        assert_eq!(
            classify_failure(&unanswered().unwrap_err()),
            FailureClass::Unanswered
        );
    }

    #[test]
    fn a_broker_rejection_counts_as_an_attempt() {
        assert_eq!(
            classify_failure(&rejected().unwrap_err()),
            FailureClass::Rejected
        );
    }

    #[test]
    fn an_envelope_payload_dedups_on_its_event_id() {
        let event_id = Uuid::now_v7();
        let payload = serde_json::json!({ "event_id": event_id.to_string() });
        assert_eq!(
            message_id_for(Uuid::nil(), &payload),
            (event_id, MessageIdSource::Envelope)
        );
    }

    #[test]
    fn a_command_envelope_dedups_on_its_command_id() {
        let command_id = Uuid::now_v7();
        let payload = serde_json::json!({ "command_id": command_id.to_string() });
        assert_eq!(
            message_id_for(Uuid::nil(), &payload),
            (command_id, MessageIdSource::Envelope)
        );
    }

    #[test]
    fn a_payload_without_an_envelope_id_falls_back_to_the_row_id() {
        let row = Uuid::now_v7();
        assert_eq!(
            message_id_for(row, &serde_json::json!({ "x": 1 })),
            (row, MessageIdSource::Row)
        );
    }
}
