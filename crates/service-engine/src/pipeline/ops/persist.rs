use crate::blobs::BlobRowOp;
use crate::error::EngineError;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::ops::Ops;
use crate::pipeline::ops::locking::{
    in_locked_order, lock_aggregate, lock_order, reconcile_key, render_key,
};
use crate::pipeline::staged::RefusalOrigin;

impl Ops<'_> {
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
            self.record_policy_refusal(super::KEY_REUSED, RefusalOrigin::CreatePrecondition);
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

    fn note_terminal(&mut self, outcome: &Result<(), EngineError>) {
        if self.staged.terminal_violation.is_some() {
            return;
        }
        if let Err(EngineError::Db(db)) = outcome
            && crate::inbound::sqlx_is_terminal(db)
        {
            self.staged.terminal_violation = Some(crate::chain::describe(db));
        }
    }
}
