use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::accumulator::{Accumulator, AccumulatorRuntime};
use crate::error::EngineError;
use crate::impact::{Dims, Impact};
use crate::inbound::ReactionMessage;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::outbound::{OutboundCommand, OutboundEvent, command_record, event_record};
use crate::pipeline::staged::{ScheduledMessage, Staged};
use crate::time::Timestamp;
use crate::wire::{Cause, Noun, encode_key};

pub struct Ops<'a> {
    pub(crate) conn: &'a mut PgConnection,
    pub(crate) staged: &'a mut Staged,
    pub(crate) accumulators: &'a AccumulatorRuntime,
    pub(crate) now: Timestamp,
}

impl<'a> Ops<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        staged: &'a mut Staged,
        accumulators: &'a AccumulatorRuntime,
        now: Timestamp,
    ) -> Self {
        Self {
            conn,
            staged,
            accumulators,
            now,
        }
    }

    pub fn now(&self) -> Timestamp {
        self.now
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.conn
    }

    pub async fn load<A: Aggregate>(
        &mut self,
        key: &<A::Store as Persistence>::Key,
    ) -> Result<Option<A>, EngineError> {
        A::Store::load(self.conn, key).await
    }

    pub async fn save<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let outcome = A::Store::save(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome
    }

    pub async fn create<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let outcome = A::Store::create(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome
    }

    fn note_terminal(&mut self, outcome: &Result<(), EngineError>) {
        if self.staged.terminal_violation.is_some() {
            return;
        }
        if let Err(EngineError::Db(db)) = outcome
            && crate::inbound::sqlx_is_terminal(db)
        {
            self.staged.terminal_violation = Some(db.to_string());
        }
    }

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

    pub fn impact_at<N: Noun>(&mut self, at: Timestamp, key: &N::Key) -> Result<(), EngineError> {
        self.staged
            .scheduled_impacts
            .push((N::NAME, encode_key::<N>(key)?, at));
        Ok(())
    }

    pub fn emit<E: OutboundEvent>(&mut self, event: E) -> Result<(), EngineError> {
        self.staged.outbox.push(event_record(&event)?);
        Ok(())
    }

    pub fn command<C: OutboundCommand>(&mut self, command: C) -> Result<(), EngineError> {
        self.staged.outbox.push(command_record(&command)?);
        Ok(())
    }

    pub fn schedule_at<M: ReactionMessage + Serialize>(
        &mut self,
        at: Timestamp,
        message: M,
    ) -> Result<(), EngineError> {
        let coordinates = M::coordinates();
        let payload = serde_json::to_vec(&message).map_err(|source| EngineError::Encode {
            what: "scheduled message",
            source,
        })?;
        self.staged.scheduled_messages.push(ScheduledMessage {
            at,
            source: coordinates.source(),
            subject: coordinates.subject(),
            message_id: Uuid::now_v7(),
            payload,
        });
        Ok(())
    }

    pub async fn seal<A: Accumulator>(
        &mut self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<(), EngineError> {
        self.accumulators.seal::<A>(self.conn, key).await
    }

    pub fn blob(&mut self) -> Result<(), EngineError> {
        Err(EngineError::NotYet { capability: "blob" })
    }
}
