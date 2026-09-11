use chrono::Utc;
use serde::Serialize;
use uuid::Uuid;

use br_core_integration::{
    Actor, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent,
};

use crate::error::EngineError;
use crate::inbound::ReactionCoordinates;
use crate::nats::{command_subject, event_subject};
use crate::relays::outbox::{OutboundSequence, OutboxRecord};

pub struct ProducerSequence {
    pub key: String,
    pub version: u64,
}

pub trait OutboundEvent: Serialize + Send + Sync + 'static {
    fn coords(&self) -> EventCoords;
    fn event_id(&self) -> Uuid;
    fn sequence(&self) -> Option<ProducerSequence> {
        None
    }
}

pub trait OutboundCommand: Serialize + Send + Sync + 'static {
    fn coords(&self) -> CommandCoords;
    fn command_id(&self) -> Uuid;
    fn sequence(&self) -> Option<ProducerSequence> {
        None
    }
}

#[derive(Clone)]
pub(crate) struct OutboundContext {
    pub actor: Actor,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub producer: Option<String>,
}

impl OutboundContext {
    fn metadata(&self) -> EventMetadata {
        let base = EventMetadata::new(self.actor, self.correlation_id);
        match self.causation_id {
            Some(causation_id) => base.with_causation(causation_id),
            None => base,
        }
    }

    fn sequence(
        &self,
        seq: Option<ProducerSequence>,
        kind: &'static str,
    ) -> Result<Option<OutboundSequence>, EngineError> {
        let Some(seq) = seq else {
            return Ok(None);
        };
        let Some(producer) = self.producer.as_ref() else {
            return Err(EngineError::Config(format!(
                "an outbound {kind} declares a producer sequence but no service is configured, so \
                 the sequence would be silently dropped and consumers could not order or dedup it; \
                 call EngineConfig::with_service to name the producer"
            )));
        };
        Ok(Some(OutboundSequence {
            producer: producer.clone(),
            seq_key: seq.key,
            seq: seq.version,
        }))
    }
}

pub(crate) fn event_record<E: OutboundEvent>(
    event: &E,
    ctx: &OutboundContext,
) -> Result<OutboxRecord, EngineError> {
    let coords = event.coords();
    let subject = event_subject(&coords);
    let event_type = format!("{}.{}", coords.aggregate.as_str(), coords.fact.as_str());
    let envelope = IntegrationEvent::new(
        event.event_id(),
        event_type,
        coords.version,
        Utc::now(),
        ctx.metadata(),
        event,
    );
    let payload = serde_json::to_value(&envelope).map_err(|source| EngineError::Encode {
        what: "outbound event",
        source,
    })?;
    Ok(OutboxRecord::stage_to(
        Uuid::now_v7(),
        subject,
        payload,
        ctx.sequence(event.sequence(), "event")?,
    ))
}

pub(crate) fn scheduled_payload<M: Serialize>(
    coordinates: &ReactionCoordinates,
    id: Uuid,
    ctx: &OutboundContext,
    message: &M,
) -> Result<serde_json::Value, EngineError> {
    let value = match coordinates {
        ReactionCoordinates::Command(coords) => {
            let command_type = format!("{}.{}", coords.aggregate.as_str(), coords.verb.as_str());
            serde_json::to_value(IntegrationCommand::new(
                id,
                command_type,
                coords.version,
                Utc::now(),
                ctx.metadata(),
                message,
            ))
        }
        ReactionCoordinates::Event(coords) => {
            let event_type = format!("{}.{}", coords.aggregate.as_str(), coords.fact.as_str());
            serde_json::to_value(IntegrationEvent::new(
                id,
                event_type,
                coords.version,
                Utc::now(),
                ctx.metadata(),
                message,
            ))
        }
    };
    value.map_err(|source| EngineError::Encode {
        what: "scheduled message",
        source,
    })
}

pub(crate) fn command_record<C: OutboundCommand>(
    command: &C,
    ctx: &OutboundContext,
) -> Result<OutboxRecord, EngineError> {
    let coords = command.coords();
    let subject = command_subject(&coords);
    let command_type = format!("{}.{}", coords.aggregate.as_str(), coords.verb.as_str());
    let envelope = IntegrationCommand::new(
        command.command_id(),
        command_type,
        coords.version,
        Utc::now(),
        ctx.metadata(),
        command,
    );
    let payload = serde_json::to_value(&envelope).map_err(|source| EngineError::Encode {
        what: "outbound command",
        source,
    })?;
    Ok(OutboxRecord::stage_to(
        Uuid::now_v7(),
        subject,
        payload,
        ctx.sequence(command.sequence(), "command")?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(producer: Option<&str>) -> OutboundContext {
        OutboundContext {
            actor: crate::identity::service_actor(producer.unwrap_or("")),
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            producer: producer.map(str::to_string),
        }
    }

    fn producer_sequence() -> ProducerSequence {
        ProducerSequence {
            key: "chat-1".to_string(),
            version: 7,
        }
    }

    #[test]
    fn a_declared_sequence_without_a_service_fails_loud_instead_of_being_dropped() {
        let error = context(None)
            .sequence(Some(producer_sequence()), "event")
            .expect_err("a producer sequence with no service must fail loud");
        assert!(matches!(error, EngineError::Config(_)));
    }

    #[test]
    fn a_declared_sequence_with_a_service_is_carried() {
        let carried = context(Some("chat"))
            .sequence(Some(producer_sequence()), "event")
            .expect("a producer sequence with a service is carried")
            .expect("the sequence is present");
        assert_eq!(carried.producer, "chat");
        assert_eq!(carried.seq_key, "chat-1");
        assert_eq!(carried.seq, 7);
    }

    #[test]
    fn no_declared_sequence_is_none_whether_or_not_a_service_is_set() {
        assert!(
            context(None)
                .sequence(None, "command")
                .expect("no sequence is always fine")
                .is_none()
        );
        assert!(
            context(Some("chat"))
                .sequence(None, "command")
                .expect("no sequence is always fine")
                .is_none()
        );
    }
}
