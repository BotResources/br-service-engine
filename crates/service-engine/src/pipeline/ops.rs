use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::{BlobHandle, BlobRef, BlobRowOp};
use crate::error::EngineError;
use crate::gate::Reason;
use crate::impact::{Deps, Dims, Impact};
use crate::inbound::ReactionMessage;
use crate::offers::OfferStagers;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::outbound::{
    OutboundCommand, OutboundContext, OutboundEvent, command_record, event_record,
};
use crate::pipeline::policy::{AggregatePolicy, Policies, PostSave, Refused, Saved};
use crate::pipeline::staged::{RefusalOrigin, ScheduledMessage, Staged, StagedRefusal};
use crate::principal::PrincipalId;
use crate::relays::outbox::OutboxRecord;
use crate::time::Timestamp;
use crate::wire::{Cause, Noun, encode_key};

const KEY_REUSED: Reason = Reason::new("KEY_REUSED");

pub struct Ops<'a> {
    pub(crate) conn: &'a mut PgConnection,
    pub(crate) staged: &'a mut Staged,
    pub(crate) accumulators: &'a AccumulatorRuntime,
    pub(crate) offers: Arc<OfferStagers>,
    pub(crate) policies: Arc<Policies>,
    pub(crate) blobs: Option<&'a BlobHandle>,
    pub(crate) now: Timestamp,
    outbound: Option<OutboundContext>,
    blob_seen: HashMap<(TypeId, Vec<u8>), Vec<BlobRef>>,
    loaded: HashMap<(TypeId, Vec<u8>), Box<dyn Any + Send + Sync>>,
}

impl<'a> Ops<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        staged: &'a mut Staged,
        accumulators: &'a AccumulatorRuntime,
        offers: Arc<OfferStagers>,
        policies: Arc<Policies>,
        blobs: Option<&'a BlobHandle>,
        now: Timestamp,
    ) -> Self {
        Self {
            conn,
            staged,
            accumulators,
            offers,
            policies,
            blobs,
            now,
            outbound: None,
            blob_seen: HashMap::new(),
            loaded: HashMap::new(),
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
        lock_aggregate::<A>(self.conn, key).await?;
        let loaded = A::Store::load(self.conn, key).await?;
        if let Some(aggregate) = &loaded {
            self.remember::<A>(aggregate)?;
        }
        Ok(loaded)
    }

    pub async fn load_many<A: Aggregate>(
        &mut self,
        keys: &[<A::Store as Persistence>::Key],
    ) -> Result<Vec<A>, EngineError> {
        let ordered = lock_order::<A>(keys)?;
        for (key, _) in &ordered {
            lock_aggregate::<A>(self.conn, key).await?;
        }
        let sorted_keys: Vec<_> = ordered.into_iter().map(|(key, _)| key).collect();
        let loaded = A::Store::read_many(self.conn, &sorted_keys).await?;
        let ordered_aggregates = in_locked_order::<A>(loaded, &sorted_keys);
        let mut aggregates = Vec::with_capacity(ordered_aggregates.len());
        for aggregate in ordered_aggregates {
            self.remember::<A>(&aggregate)?;
            aggregates.push(aggregate);
        }
        Ok(aggregates)
    }

    pub async fn save<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let outcome = A::Store::save(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome?;
        self.run_save_policy::<A>(aggregate)?;
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        self.reconcile_blobs::<A>(aggregate)?;
        self.remember::<A>(aggregate)
    }

    pub async fn create<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let key = aggregate.key();
        lock_aggregate::<A>(self.conn, &key).await?;
        if A::Store::load(self.conn, &key).await?.is_some() {
            self.record_policy_refusal(KEY_REUSED, RefusalOrigin::CreatePrecondition);
            return Err(EngineError::KeyReused {
                store: std::any::type_name::<A::Store>(),
                key: render_key::<A>(&key),
            });
        }
        let outcome = A::Store::create(self.conn, aggregate, aggregate.pending_events()).await;
        self.note_terminal(&outcome);
        outcome?;
        self.run_create_policy::<A>(aggregate)?;
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        self.reconcile_blobs::<A>(aggregate)?;
        self.remember::<A>(aggregate)
    }

    pub async fn delete<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        self.run_delete_policy::<A>(aggregate)?;
        let key = aggregate.key();
        let outcome = A::Store::delete(self.conn, &key).await;
        self.note_terminal(&outcome);
        outcome?;
        self.offers
            .stage_for(aggregate, &mut self.staged.offer_dirty)?;
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        self.loaded.remove(&reconcile_key);
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

    fn run_save_policy<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let Some(policy) = self.policies.save.get::<A>() else {
            return Ok(());
        };
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        let prior: Option<A> = self
            .loaded
            .get(&reconcile_key)
            .and_then(|held| held.downcast_ref::<A>())
            .cloned();
        self.dispatch_policy::<A>(
            policy,
            prior.as_ref(),
            aggregate,
            RefusalOrigin::PostSavePolicy,
        )
    }

    fn run_create_policy<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let Some(policy) = self.policies.save.get::<A>() else {
            return Ok(());
        };
        self.dispatch_policy::<A>(policy, None, aggregate, RefusalOrigin::PostSavePolicy)
    }

    fn run_delete_policy<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let Some(policy) = self.policies.delete.get::<A>() else {
            return Ok(());
        };
        self.dispatch_policy::<A>(
            policy,
            Some(aggregate),
            aggregate,
            RefusalOrigin::PostDeletePolicy,
        )
    }

    fn dispatch_policy<A: Aggregate>(
        &mut self,
        policy: Arc<dyn AggregatePolicy<A>>,
        prior: Option<&A>,
        aggregate: &A,
        origin: RefusalOrigin,
    ) -> Result<(), EngineError> {
        let saved = Saved {
            prior,
            next: aggregate,
            events: aggregate.pending_events(),
        };
        let outcome = {
            let mut post_save = PostSave::new(self, origin);
            policy.run(saved, &mut post_save)
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(Refused(reason)) => {
                self.record_policy_refusal(reason, origin);
                Err(EngineError::PolicyRefused {
                    code: reason.code(),
                })
            }
        }
    }

    pub(crate) fn record_policy_refusal(&mut self, reason: Reason, origin: RefusalOrigin) {
        if self.staged.policy_refusal.is_none() {
            self.staged.policy_refusal = Some(StagedRefusal { reason, origin });
        }
    }

    fn remember<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        self.blob_seen
            .insert(reconcile_key.clone(), aggregate.blob_refs());
        self.loaded
            .insert(reconcile_key, Box::new(aggregate.clone()));
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

fn key_bytes<A: Aggregate>(key: &<A::Store as Persistence>::Key) -> Result<Vec<u8>, EngineError> {
    serde_json::to_vec(key).map_err(|source| EngineError::Encode {
        what: "aggregate key for the load advisory lock",
        source,
    })
}

fn render_key<A: Aggregate>(key: &<A::Store as Persistence>::Key) -> String {
    serde_json::to_string(key).unwrap_or_else(|_| "<unrenderable key>".to_string())
}

type KeyWithBytes<A> = (<<A as Aggregate>::Store as Persistence>::Key, Vec<u8>);

fn lock_order<A: Aggregate>(
    keys: &[<A::Store as Persistence>::Key],
) -> Result<Vec<KeyWithBytes<A>>, EngineError> {
    let mut ordered: Vec<KeyWithBytes<A>> = Vec::with_capacity(keys.len());
    for key in keys {
        let bytes = key_bytes::<A>(key)?;
        if ordered.iter().any(|(_, seen)| *seen == bytes) {
            continue;
        }
        ordered.push((key.clone(), bytes));
    }
    ordered.sort_by(|(_, a), (_, b)| a.cmp(b));
    Ok(ordered)
}

fn in_locked_order<A: Aggregate>(
    loaded: Vec<(<A::Store as Persistence>::Key, A)>,
    order: &[<A::Store as Persistence>::Key],
) -> Vec<A> {
    let mut by_key: HashMap<<A::Store as Persistence>::Key, A> = loaded.into_iter().collect();
    order.iter().filter_map(|key| by_key.remove(key)).collect()
}

async fn lock_aggregate<A: Aggregate>(
    conn: &mut PgConnection,
    key: &<A::Store as Persistence>::Key,
) -> Result<(), EngineError> {
    let store = std::any::type_name::<A::Store>();
    let bytes = key_bytes::<A>(key)?;
    let id = crate::advisory::lock_id(crate::advisory::AGGREGATE_LOAD, &[store.as_bytes(), &bytes]);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(EngineError::Db)?;
    A::Store::lock(conn, key).await
}

fn reconcile_key<A: Aggregate>(aggregate: &A) -> Result<(TypeId, Vec<u8>), EngineError> {
    let key = serde_json::to_vec(&aggregate.key()).map_err(|source| EngineError::Encode {
        what: "aggregate key for blob reconciliation",
        source,
    })?;
    Ok((TypeId::of::<A>(), key))
}

#[cfg(test)]
mod lock_order_tests {
    use super::*;
    use crate::persistence::PersistenceStyle;
    use futures_util::future::BoxFuture;

    #[derive(Clone)]
    struct Widget(String);
    struct WidgetStore;

    impl Persistence for WidgetStore {
        type Aggregate = Widget;
        type Key = String;
        type Event = ();
        const STYLE: PersistenceStyle = PersistenceStyle::Crud;

        fn load<'a>(
            _conn: &'a mut PgConnection,
            _key: &'a Self::Key,
        ) -> BoxFuture<'a, Result<Option<Self::Aggregate>, EngineError>> {
            Box::pin(async { unimplemented!("lock_order never loads") })
        }
        fn save<'a>(
            _conn: &'a mut PgConnection,
            _aggregate: &'a Self::Aggregate,
            _events: &'a [Self::Event],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { unimplemented!("lock_order never saves") })
        }
        fn create<'a>(
            _conn: &'a mut PgConnection,
            _aggregate: &'a Self::Aggregate,
            _events: &'a [Self::Event],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { unimplemented!("lock_order never creates") })
        }
    }

    impl Aggregate for Widget {
        type Store = WidgetStore;
        fn key(&self) -> String {
            self.0.clone()
        }
    }

    fn order(keys: &[&str]) -> Vec<String> {
        lock_order::<Widget>(&keys.iter().map(|k| k.to_string()).collect::<Vec<_>>())
            .expect("string keys always encode")
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    #[test]
    fn keys_are_locked_in_ascending_encoded_order_whatever_the_input_order() {
        assert_eq!(order(&["c", "a", "b"]), vec!["a", "b", "c"]);
        assert_eq!(order(&["b", "c", "a"]), vec!["a", "b", "c"]);
    }

    #[test]
    fn a_repeated_key_is_locked_once() {
        assert_eq!(order(&["a", "b", "a", "b", "a"]), vec!["a", "b"]);
    }

    #[test]
    fn the_empty_set_locks_nothing() {
        assert!(order(&[]).is_empty());
    }

    fn widget(key: &str) -> Widget {
        Widget(key.to_string())
    }

    #[test]
    fn a_batch_returned_out_of_order_is_re_emitted_in_the_locked_order() {
        let loaded = vec![
            ("c".to_string(), widget("c")),
            ("a".to_string(), widget("a")),
            ("b".to_string(), widget("b")),
        ];
        let order = ["a".to_string(), "b".to_string(), "c".to_string()];
        let result: Vec<_> = in_locked_order::<Widget>(loaded, &order)
            .into_iter()
            .map(|w| w.0)
            .collect();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn an_absent_key_is_omitted_and_the_rest_keep_the_locked_order() {
        let loaded = vec![
            ("c".to_string(), widget("c")),
            ("a".to_string(), widget("a")),
        ];
        let order = ["a".to_string(), "b".to_string(), "c".to_string()];
        let result: Vec<_> = in_locked_order::<Widget>(loaded, &order)
            .into_iter()
            .map(|w| w.0)
            .collect();
        assert_eq!(result, vec!["a", "c"]);
    }
}
