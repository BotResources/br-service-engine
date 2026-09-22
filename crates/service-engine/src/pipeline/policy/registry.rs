use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::EngineError;
use crate::persistence::Aggregate;
use crate::pipeline::policy::{PostSave, Refused, Saved};

type PolicyOutcome = Result<(), Refused>;

pub(crate) trait AggregatePolicy<A: Aggregate>: Send + Sync {
    fn run(&self, saved: Saved<'_, A>, ps: &mut PostSave<'_, '_>) -> PolicyOutcome;
}

struct PolicyFor<A, F> {
    policy: F,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A, F> AggregatePolicy<A> for PolicyFor<A, F>
where
    A: Aggregate,
    F: Fn(Saved<'_, A>, &mut PostSave<'_, '_>) -> PolicyOutcome + Send + Sync + 'static,
{
    fn run(&self, saved: Saved<'_, A>, ps: &mut PostSave<'_, '_>) -> PolicyOutcome {
        (self.policy)(saved, ps)
    }
}

#[derive(Default, Clone)]
pub(crate) struct AggregatePolicies {
    by_aggregate: BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl AggregatePolicies {
    pub(crate) fn register<A, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        A: Aggregate,
        F: Fn(Saved<'_, A>, &mut PostSave<'_, '_>) -> PolicyOutcome + Send + Sync + 'static,
    {
        if self.by_aggregate.contains_key(&TypeId::of::<A>()) {
            return Err(EngineError::Config(format!(
                "a policy is already registered for aggregate {}",
                std::any::type_name::<A>()
            )));
        }
        let erased: Arc<dyn AggregatePolicy<A>> = Arc::new(PolicyFor::<A, F> {
            policy,
            _aggregate: PhantomData,
        });
        self.by_aggregate
            .insert(TypeId::of::<A>(), Arc::new(erased));
        Ok(())
    }

    pub(crate) fn contains(&self, aggregate: TypeId) -> bool {
        self.by_aggregate.contains_key(&aggregate)
    }

    pub(crate) fn get<A: Aggregate>(&self) -> Option<Arc<dyn AggregatePolicy<A>>> {
        self.by_aggregate
            .get(&TypeId::of::<A>())
            .and_then(|erased| erased.downcast_ref::<Arc<dyn AggregatePolicy<A>>>())
            .cloned()
    }
}

#[derive(Default, Clone)]
pub(crate) struct Policies {
    pub(crate) save: AggregatePolicies,
    pub(crate) delete: AggregatePolicies,
    pub(crate) upload: crate::blobs::policy::UploadPolicies,
}

#[derive(Default)]
pub(crate) struct PolicySeams {
    save: Vec<(TypeId, &'static str)>,
    delete: Vec<(TypeId, &'static str)>,
    upload: Vec<(&'static str, &'static str)>,
}

impl PolicySeams {
    pub(crate) fn require_save<A: Aggregate>(&mut self) {
        require::<A>(&mut self.save);
    }

    pub(crate) fn require_delete<A: Aggregate>(&mut self) {
        require::<A>(&mut self.delete);
    }

    pub(crate) fn require_upload<B: crate::blobs::Blobs>(&mut self) {
        if !self.upload.iter().any(|(kind, _)| *kind == B::KIND) {
            self.upload.push((B::KIND, std::any::type_name::<B>()));
        }
    }

    pub(crate) fn verify(
        &self,
        save: &AggregatePolicies,
        delete: &AggregatePolicies,
        upload: &crate::blobs::policy::UploadPolicies,
    ) -> Result<(), EngineError> {
        verify_against(&self.save, save)?;
        verify_against(&self.delete, delete)?;
        for (kind, aggregate) in &self.upload {
            if !upload.contains(kind) {
                return Err(EngineError::UnhonouredSeam { aggregate });
            }
        }
        Ok(())
    }
}

fn require<A: Aggregate>(into: &mut Vec<(TypeId, &'static str)>) {
    let type_id = TypeId::of::<A>();
    if !into.iter().any(|(id, _)| *id == type_id) {
        into.push((type_id, std::any::type_name::<A>()));
    }
}

fn verify_against(
    required: &[(TypeId, &'static str)],
    policies: &AggregatePolicies,
) -> Result<(), EngineError> {
    for (type_id, aggregate) in required {
        if !policies.contains(*type_id) {
            return Err(EngineError::UnhonouredSeam { aggregate });
        }
    }
    Ok(())
}
