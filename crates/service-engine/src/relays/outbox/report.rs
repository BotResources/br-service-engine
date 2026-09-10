use br_core_integration::{OutboxStatus, Transition};
use uuid::Uuid;

use crate::nats::{NatsError, PublishFailure, PublishOutcome};

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
    pub min_retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Structural,
    Transient,
}

pub fn classify_failure(err: &NatsError) -> FailureClass {
    match err {
        NatsError::Publish {
            kind: PublishFailure::NoStream,
            ..
        } => FailureClass::Structural,
        _ => FailureClass::Transient,
    }
}

pub(super) fn classify_pass(
    pass: &mut RelayPass,
    publish_result: &Result<PublishOutcome, NatsError>,
    transition: Transition,
    structural: bool,
) {
    if let Ok(outcome) = publish_result {
        pass.published += 1;
        if outcome.is_duplicate() {
            pass.duplicates += 1;
        }
        return;
    }
    if structural {
        pass.structural += 1;
        return;
    }
    if transition.status == OutboxStatus::Failed {
        pass.failed += 1;
    } else {
        pass.retried += 1;
        pass.min_retry_attempts = Some(match pass.min_retry_attempts {
            Some(prev) => prev.min(transition.attempts),
            None => transition.attempts,
        });
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

    fn stored() -> Result<PublishOutcome, NatsError> {
        Ok(PublishOutcome::Stored { sequence: 1 })
    }

    fn duplicate() -> Result<PublishOutcome, NatsError> {
        Ok(PublishOutcome::Duplicate { sequence: 1 })
    }

    fn no_stream() -> Result<PublishOutcome, NatsError> {
        Err(NatsError::Publish {
            subject: "integration.evt.sample.assignment.relayed.v1".into(),
            kind: PublishFailure::NoStream,
            detail: "no stream".into(),
        })
    }

    fn transient() -> Result<PublishOutcome, NatsError> {
        Err(NatsError::Publish {
            subject: "integration.evt.sample.assignment.relayed.v1".into(),
            kind: PublishFailure::Transient,
            detail: "timed out".into(),
        })
    }

    fn pending(attempts: u32) -> Transition {
        Transition {
            status: OutboxStatus::Pending,
            attempts,
        }
    }

    #[test]
    fn no_stream_is_structural() {
        assert_eq!(
            classify_failure(&no_stream().unwrap_err()),
            FailureClass::Structural
        );
    }

    #[test]
    fn timeout_is_transient() {
        assert_eq!(
            classify_failure(&transient().unwrap_err()),
            FailureClass::Transient
        );
    }

    #[test]
    fn a_duplicate_ack_counts_as_published_and_as_a_duplicate() {
        let mut pass = RelayPass::default();
        classify_pass(&mut pass, &duplicate(), pending(1), false);
        assert_eq!(pass.published, 1);
        assert_eq!(pass.duplicates, 1);
    }

    #[test]
    fn a_plain_success_is_published_and_not_a_duplicate() {
        let mut pass = RelayPass::default();
        classify_pass(&mut pass, &stored(), pending(1), false);
        assert_eq!(pass.published, 1);
        assert_eq!(pass.duplicates, 0);
    }

    #[test]
    fn a_structural_failure_does_not_burn_retry() {
        let mut pass = RelayPass::default();
        classify_pass(&mut pass, &no_stream(), pending(0), true);
        assert_eq!(pass.structural, 1);
        assert_eq!(pass.retried, 0);
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
