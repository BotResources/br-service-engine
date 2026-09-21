use serde::Serialize;
use uuid::Uuid;

use crate::error::EngineError;
use crate::impact::{Deps, Dims, Impact};
use crate::inbound::ReactionMessage;
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::{OutboundCommand, OutboundEvent, command_record, event_record};
use crate::pipeline::staged::ScheduledMessage;
use crate::principal::PrincipalId;
use crate::relays::outbox::OutboxRecord;
use crate::time::Timestamp;
use crate::wire::{Cause, Noun, encode_key};

impl Ops<'_> {
    pub fn impact<N: Noun>(&mut self, key: &N::Key, dims: Dims) -> Result<(), EngineError> {
        self.staged.impacts.push(Impact::resource::<N>(key, dims)?);
        Ok(())
    }

    pub fn impact_caused<N: Noun, C: Serialize>(
        &mut self,
        key: &N::Key,
        cause: C,
    ) -> Result<(), EngineError> {
        let cause = Cause::encode(&cause)?;
        self.staged
            .impacts
            .push(Impact::resource_caused::<N>(key, Dims::ALL, cause)?);
        Ok(())
    }

    pub fn impact_principal_facts(&mut self, principal: PrincipalId, deps: Deps) {
        self.staged
            .impacts
            .push(Impact::principal_facts(principal, deps));
    }

    pub fn impact_at<N: Noun>(&mut self, at: Timestamp, key: &N::Key) -> Result<(), EngineError> {
        self.staged
            .scheduled_impacts
            .push((N::NAME, encode_key::<N>(key)?, at));
        Ok(())
    }

    pub fn emit<E: OutboundEvent>(&mut self, event: E) -> Result<(), EngineError> {
        let record = self.outbound().and_then(|ctx| event_record(&event, ctx));
        self.push_outbound(record)
    }

    pub fn command<C: OutboundCommand>(&mut self, command: C) -> Result<(), EngineError> {
        let record = self
            .outbound()
            .and_then(|ctx| command_record(&command, ctx));
        self.push_outbound(record)
    }

    fn push_outbound(
        &mut self,
        record: Result<OutboxRecord, EngineError>,
    ) -> Result<(), EngineError> {
        let record = record.inspect_err(|error| self.note_config_terminal(error))?;
        self.staged.outbox.push(record);
        Ok(())
    }

    fn note_config_terminal(&mut self, error: &EngineError) {
        if self.staged.terminal_violation.is_none() && matches!(error, EngineError::Config(_)) {
            self.staged.terminal_violation = Some(error.to_string());
        }
    }

    pub fn schedule_at<M: ReactionMessage + Serialize>(
        &mut self,
        at: Timestamp,
        message: M,
    ) -> Result<(), EngineError> {
        let coordinates = M::coordinates();
        let id = Uuid::now_v7();
        let envelope = crate::pipeline::outbound::scheduled_payload(
            &coordinates,
            id,
            self.outbound()?,
            &message,
        )?;
        let payload = serde_json::to_vec(&envelope).map_err(|source| EngineError::Encode {
            what: "scheduled message",
            source,
        })?;
        self.staged.scheduled_messages.push(ScheduledMessage {
            at,
            source: coordinates.source(),
            subject: coordinates.subject(),
            message_id: id,
            payload,
        });
        Ok(())
    }
}
