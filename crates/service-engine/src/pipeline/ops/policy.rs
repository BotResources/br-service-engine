use std::sync::Arc;

use crate::blobs::BlobRowOp;
use crate::error::EngineError;
use crate::gate::Reason;
use crate::persistence::Aggregate;
use crate::pipeline::ops::Ops;
use crate::pipeline::ops::locking::reconcile_key;
use crate::pipeline::policy::{AggregatePolicy, PostSave, Refused, Saved};
use crate::pipeline::staged::{RefusalOrigin, StagedRefusal};

impl Ops<'_> {
    pub(super) fn run_save_policy<A: Aggregate>(
        &mut self,
        aggregate: &A,
    ) -> Result<(), EngineError> {
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

    pub(super) fn run_create_policy<A: Aggregate>(
        &mut self,
        aggregate: &A,
    ) -> Result<(), EngineError> {
        let Some(policy) = self.policies.save.get::<A>() else {
            return Ok(());
        };
        self.dispatch_policy::<A>(policy, None, aggregate, RefusalOrigin::PostSavePolicy)
    }

    pub(super) fn run_delete_policy<A: Aggregate>(
        &mut self,
        aggregate: &A,
    ) -> Result<(), EngineError> {
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

    pub(super) fn remember<A: Aggregate>(&mut self, aggregate: &A) -> Result<(), EngineError> {
        let reconcile_key = reconcile_key::<A>(aggregate)?;
        self.blob_seen
            .insert(reconcile_key.clone(), aggregate.blob_refs());
        self.loaded
            .insert(reconcile_key, Box::new(aggregate.clone()));
        Ok(())
    }

    pub(super) fn reconcile_blobs<A: Aggregate>(
        &mut self,
        aggregate: &A,
    ) -> Result<(), EngineError> {
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
}
