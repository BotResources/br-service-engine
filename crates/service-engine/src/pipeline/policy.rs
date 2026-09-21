use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::gate::Reason;
use crate::impact::Dims;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::{OutboundCommand, OutboundEvent};
use crate::pipeline::staged::RefusalOrigin;
use crate::time::Timestamp;
use crate::wire::Noun;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused(pub Reason);

impl From<Reason> for Refused {
    fn from(reason: Reason) -> Self {
        Self(reason)
    }
}

pub struct Saved<'a, A: Aggregate> {
    pub prior: Option<&'a A>,
    pub next: &'a A,
    pub events: &'a [<A::Store as Persistence>::Event],
}

impl<A: Aggregate> Saved<'_, A> {
    pub fn transitioned<T, F>(&self, project: F) -> bool
    where
        T: PartialEq,
        F: Fn(&A) -> T,
    {
        match self.prior {
            Some(prior) => project(prior) != project(self.next),
            None => true,
        }
    }
}

pub struct PostSave<'a, 'ops> {
    ops: &'a mut Ops<'ops>,
    origin: RefusalOrigin,
}

impl<'a, 'ops> PostSave<'a, 'ops> {
    pub(crate) fn new(ops: &'a mut Ops<'ops>, origin: RefusalOrigin) -> Self {
        Self { ops, origin }
    }

    pub fn now(&self) -> Timestamp {
        self.ops.now()
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.ops.connection()
    }

    pub fn impact<N: Noun>(&mut self, key: &N::Key, dims: Dims) -> Result<(), EngineError> {
        self.ops.impact::<N>(key, dims)
    }

    pub fn impact_caused<N: Noun, C: Serialize>(
        &mut self,
        key: &N::Key,
        cause: C,
    ) -> Result<(), EngineError> {
        self.ops.impact_caused::<N, C>(key, cause)
    }

    pub fn command<C: OutboundCommand>(&mut self, command: C) -> Result<(), EngineError> {
        self.ops.command(command)
    }

    pub fn emit<E: OutboundEvent>(&mut self, event: E) -> Result<(), EngineError> {
        self.ops.emit(event)
    }

    pub fn refuse(&mut self, reason: Reason) -> Refused {
        self.ops.record_policy_refusal(reason, self.origin);
        Refused(reason)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{Persistence, PersistenceStyle};
    use futures_util::future::BoxFuture;
    use sqlx::PgConnection;

    #[derive(Clone)]
    struct Alpha;
    struct AlphaStore;
    #[derive(Clone)]
    struct Beta;
    struct BetaStore;

    macro_rules! dummy_store {
        ($store:ty, $agg:ty) => {
            impl Persistence for $store {
                type Aggregate = $agg;
                type Key = u32;
                type Event = ();
                const STYLE: PersistenceStyle = PersistenceStyle::Crud;
                fn load<'a>(
                    _c: &'a mut PgConnection,
                    _k: &'a u32,
                ) -> BoxFuture<'a, Result<Option<$agg>, EngineError>> {
                    Box::pin(async { unimplemented!() })
                }
                fn save<'a>(
                    _c: &'a mut PgConnection,
                    _a: &'a $agg,
                    _e: &'a [()],
                ) -> BoxFuture<'a, Result<(), EngineError>> {
                    Box::pin(async { unimplemented!() })
                }
                fn create<'a>(
                    _c: &'a mut PgConnection,
                    _a: &'a $agg,
                    _e: &'a [()],
                ) -> BoxFuture<'a, Result<(), EngineError>> {
                    Box::pin(async { unimplemented!() })
                }
            }
            impl Aggregate for $agg {
                type Store = $store;
                fn key(&self) -> u32 {
                    0
                }
            }
        };
    }
    dummy_store!(AlphaStore, Alpha);
    dummy_store!(BetaStore, Beta);

    #[test]
    fn a_second_policy_for_one_aggregate_is_refused() {
        let mut policies = AggregatePolicies::default();
        policies
            .register::<Alpha, _>(|_s, _ps| Ok(()))
            .expect("first policy registers");
        let again = policies.register::<Alpha, _>(|_s, _ps| Ok(()));
        assert!(matches!(again, Err(EngineError::Config(_))));
    }

    #[test]
    fn a_registered_policy_is_recovered_by_aggregate_type() {
        let mut policies = AggregatePolicies::default();
        policies.register::<Alpha, _>(|_s, _ps| Ok(())).unwrap();
        assert!(policies.get::<Alpha>().is_some());
        assert!(policies.get::<Beta>().is_none());
    }

    #[test]
    fn an_unhonoured_save_subjection_fails_verification_naming_the_aggregate() {
        let mut seams = PolicySeams::default();
        seams.require_save::<Alpha>();
        let policies = Policies::default();
        let error = seams.verify(&policies.save, &policies.delete, &policies.upload).unwrap_err();
        assert!(
            matches!(error, EngineError::UnhonouredSeam { aggregate } if aggregate.contains("Alpha"))
        );
    }

    #[test]
    fn an_unhonoured_delete_subjection_fails_verification_naming_the_aggregate() {
        let mut seams = PolicySeams::default();
        seams.require_delete::<Beta>();
        let mut policies = Policies::default();
        policies.save.register::<Beta, _>(|_s, _ps| Ok(())).unwrap();
        let error = seams.verify(&policies.save, &policies.delete, &policies.upload).unwrap_err();
        assert!(
            matches!(error, EngineError::UnhonouredSeam { aggregate } if aggregate.contains("Beta")),
            "a save policy does not honour a delete subjection"
        );
    }

    #[test]
    fn a_subjection_honoured_by_a_registered_policy_verifies() {
        let mut seams = PolicySeams::default();
        seams.require_save::<Alpha>();
        seams.require_save::<Alpha>();
        seams.require_delete::<Alpha>();
        let mut policies = Policies::default();
        policies
            .save
            .register::<Alpha, _>(|_s, _ps| Ok(()))
            .unwrap();
        policies
            .delete
            .register::<Alpha, _>(|_s, _ps| Ok(()))
            .unwrap();
        assert!(seams.verify(&policies.save, &policies.delete, &policies.upload).is_ok());
    }
}
