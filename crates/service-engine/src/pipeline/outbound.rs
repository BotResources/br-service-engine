use serde::Serialize;
use uuid::Uuid;

use br_core_integration::{CommandCoords, EventCoords};

use crate::error::EngineError;
use crate::nats::{command_subject, event_subject};
use crate::relays::outbox::OutboxRecord;

pub trait OutboundEvent: Serialize + Send + Sync + 'static {
    fn coords(&self) -> EventCoords;
    fn event_id(&self) -> Uuid;
}

pub trait OutboundCommand: Serialize + Send + Sync + 'static {
    fn coords(&self) -> CommandCoords;
    fn command_id(&self) -> Uuid;
}

pub(crate) fn event_record<E: OutboundEvent>(event: &E) -> Result<OutboxRecord, EngineError> {
    let subject = event_subject(&event.coords());
    let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
        what: "outbound event",
        source,
    })?;
    Ok(OutboxRecord::stage_to(event.event_id(), subject, payload))
}

pub(crate) fn command_record<C: OutboundCommand>(command: &C) -> Result<OutboxRecord, EngineError> {
    let subject = command_subject(&command.coords());
    let payload = serde_json::to_value(command).map_err(|source| EngineError::Encode {
        what: "outbound command",
        source,
    })?;
    Ok(OutboxRecord::stage_to(
        command.command_id(),
        subject,
        payload,
    ))
}
