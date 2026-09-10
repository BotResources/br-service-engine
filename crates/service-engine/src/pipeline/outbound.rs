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

    fn sequence(&self, seq: Option<ProducerSequence>) -> Option<OutboundSequence> {
        let (producer, seq) = self.producer.as_ref().zip(seq)?;
        Some(OutboundSequence {
            producer: producer.clone(),
            seq_key: seq.key,
            seq: seq.version,
        })
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
        event.event_id(),
        subject,
        payload,
        ctx.sequence(event.sequence()),
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
        command.command_id(),
        subject,
        payload,
        ctx.sequence(command.sequence()),
    ))
}
