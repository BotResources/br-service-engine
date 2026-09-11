use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::{BlobHandle, BlobRef, BlobRowOp};
use crate::error::EngineError;
use crate::impact::{Deps, Dims, Impact};
use crate::inbound::ReactionMessage;
use crate::offers::OfferStagers;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::outbound::{
    OutboundCommand, OutboundContext, OutboundEvent, command_record, event_record,
};
use crate::pipeline::staged::{ScheduledMessage, Staged};
use crate::principal::PrincipalId;
use crate::relays::outbox::OutboxRecord;
use crate::time::Timestamp;
use crate::wire::{Cause, Noun, encode_key};

pub struct Ops<'a> {
    pub(crate) conn: &'a mut PgConnection,
    pub(crate) staged: &'a mut Staged,
    pub(crate) accumulators: &'a AccumulatorRuntime,
    pub(crate) offers: Arc<OfferStagers>,
    pub(crate) blobs: Option<&'a BlobHandle>,
    pub(crate) now: Timestamp,
    outbound: Option<OutboundContext>,
    blob_seen: HashMap<(TypeId, Vec<u8>), Vec<BlobRef>>,
}

impl<'a> Ops<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        staged: &'a mut Staged,
        accumulators: &'a AccumulatorRuntime,
        offers: Arc<OfferStagers>,
        blobs: Option<&'a BlobHandle>,
        now: Timestamp,
    ) -> Self {
        Self {
            conn,
            staged,
            accumulators,
            offers,
            blobs,
            now,
            outbound: None,
            blob_seen: HashMap::new(),
        }
    }

    pub(crate) fn with_outbound(mut self, outbound: OutboundContext) -> Self {
        self.outbound = Some(outbound);
        self
    }

    fn outbound(&self) -> Result<&OutboundContext, EngineError> {
        self.outbound.as_ref().ok_or_else(|| {
            EngineError::Config(
                "this pipeline has no outbound identity, so it cannot emit an integration event \
                 or command onto the frontier"
                    .into(),
            )
        })
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
        aggregate_advisory_lock::<A>(self.conn, key).await?;
        A::Store::lock(self.conn, key).await?;
        let loaded = A::Store::load(self.conn, key).await?;
        if let Some(aggregate) = &loaded {
            let reconcile_key = reconcile_key::<A>(aggregate)?;
            self.blob_seen.insert(reconcile_key, aggregate.blob_refs());
        }
        Ok(loaded)
    }

    pub async fn save<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let outcome = A::Store::save(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome?;
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        self.reconcile_blobs::<A>(aggregate)
    }

    pub async fn create<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let outcome = A::Store::create(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome?;
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        self.reconcile_blobs::<A>(aggregate)
    }

    pub fn delete<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        let mut released = self.blob_seen.remove(&reconcile_key).unwrap_or_default();
        for reference in aggregate.blob_refs() {
            if !released.contains(&reference) {
                released.push(reference);
            }
        }
        for reference in released {
            self.staged
                .blob_ops
                .push(BlobRowOp::Orphan(reference, self.now));
        }
        Ok(())
    }

    fn reconcile_blobs<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        let new_refs = aggregate.blob_refs();
        if let Some(old_refs) = self.blob_seen.get(&reconcile_key) {
            for reference in old_refs {
                if !new_refs.contains(reference) {
                    self.staged
                        .blob_ops
                        .push(BlobRowOp::Orphan(*reference, self.now));
                }
            }
        }
        self.blob_seen.insert(reconcile_key, new_refs);
        Ok(())
    }

    pub fn dirty_offer<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)
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

    fn note_config_terminal(&mut self, error: &EngineError) {
        if self.staged.terminal_violation.is_none() && matches!(error, EngineError::Config(_)) {
            self.staged.terminal_violation = Some(error.to_string());
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

async fn aggregate_advisory_lock<A: Aggregate>(
    conn: &mut PgConnection,
    key: &<A::Store as Persistence>::Key,
) -> Result<(), EngineError> {
    let store = std::any::type_name::<A::Store>();
    let key_bytes = serde_json::to_vec(key).map_err(|source| EngineError::Encode {
        what: "aggregate key for the load advisory lock",
        source,
    })?;
    let id = crate::advisory::lock_id(crate::advisory::AGGREGATE_LOAD, &[store.as_bytes(), &key_bytes]);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(id)
        .execute(conn)
        .await
        .map_err(EngineError::Db)?;
    Ok(())
}

fn reconcile_key<A: Aggregate>(aggregate: &A) -> Result<(TypeId, Vec<u8>), EngineError> {
    let key = serde_json::to_vec(&aggregate.key()).map_err(|source| EngineError::Encode {
        what: "aggregate key for blob reconciliation",
        source,
    })?;
    Ok((TypeId::of::<A>(), key))
}
