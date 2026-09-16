//! Post-save policies — the engine's answer to the cross-slice wiring that a
//! slice was meant to call but never did.
//!
//! A slice declares, once, that an aggregate is *subject* to a policy; the
//! engine then runs that policy after every `save`/`create` of the aggregate,
//! inside the same transaction, before the commit. There is no call site to add
//! at every mutation and reaction that touches the aggregate, so there is none
//! to forget — the interlock cannot be written, tested, and then called from
//! nowhere (the shape of every HIGH defect in the 0.1 rewrite).
//!
//! A policy is pure in the sense principle 18 means: it observes the aggregate
//! the pipeline just saved and produces data — a refusal, or staged impacts,
//! commands and events. Its two known uses are the Services **breach
//! interlock** (refuse marking an item implemented while it carries an open
//! breach) and the Runners **reconcile journal impact** (stage an impact for
//! the reconcile journal after a runner is saved).

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::gate::Reason;
use crate::impact::Dims;
use crate::persistence::Aggregate;
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::{OutboundCommand, OutboundEvent};
use crate::time::Timestamp;
use crate::wire::Noun;

/// The refusal a post-save policy returns to reject the write it just observed.
///
/// Obtain one from [`PostSave::refuse`] so the reason is recorded on the
/// pipeline; returning `Err(Refused(..))` then propagates it out of the policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused(pub Reason);

impl From<Reason> for Refused {
    fn from(reason: Reason) -> Self {
        Self(reason)
    }
}

/// The context a post-save policy runs in.
///
/// It can read facts through [`PostSave::connection`], stage impacts, commands
/// and events, and [`PostSave::refuse`] the write. It deliberately cannot save:
/// a policy can never trigger itself, so there is no recursion to guard.
pub struct PostSave<'a, 'ops> {
    ops: &'a mut Ops<'ops>,
}

impl<'a, 'ops> PostSave<'a, 'ops> {
    pub(crate) fn new(ops: &'a mut Ops<'ops>) -> Self {
        Self { ops }
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

    /// Refuse the write. The transaction is rolled back and the mutation answers
    /// this reason code; a reaction that refuses is dead-lettered with it.
    pub fn refuse(&mut self, reason: Reason) -> Refused {
        self.ops.record_policy_refusal(reason);
        Refused(reason)
    }
}

type PolicyOutcome = Result<(), Refused>;

pub(crate) trait ErasedPolicy: Send + Sync {
    fn run(&self, aggregate: &dyn Any, ps: &mut PostSave<'_, '_>) -> PolicyOutcome;
}

struct PolicyFor<A, F> {
    policy: F,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A, F> ErasedPolicy for PolicyFor<A, F>
where
    A: Aggregate,
    F: Fn(&A, &mut PostSave<'_, '_>) -> PolicyOutcome + Send + Sync + 'static,
{
    fn run(&self, aggregate: &dyn Any, ps: &mut PostSave<'_, '_>) -> PolicyOutcome {
        match aggregate.downcast_ref::<A>() {
            Some(aggregate) => (self.policy)(aggregate, ps),
            // Unreachable: dispatch is keyed by `TypeId::of::<A>()`, so the
            // erased value is always an `A`. Treated as a no-op rather than a
            // panic to keep the save path infallible on a would-be bug.
            None => {
                debug_assert!(
                    false,
                    "a post-save policy was dispatched on the wrong aggregate"
                );
                Ok(())
            }
        }
    }
}

/// The per-aggregate post-save policies, keyed by aggregate `TypeId`.
#[derive(Default, Clone)]
pub(crate) struct PostSavePolicies {
    by_aggregate: BTreeMap<TypeId, Arc<dyn ErasedPolicy>>,
}

impl PostSavePolicies {
    pub(crate) fn register<A, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        A: Aggregate,
        F: Fn(&A, &mut PostSave<'_, '_>) -> PolicyOutcome + Send + Sync + 'static,
    {
        if self.by_aggregate.contains_key(&TypeId::of::<A>()) {
            return Err(EngineError::Config(format!(
                "a post-save policy is already registered for aggregate {}",
                std::any::type_name::<A>()
            )));
        }
        self.by_aggregate.insert(
            TypeId::of::<A>(),
            Arc::new(PolicyFor::<A, F> {
                policy,
                _aggregate: PhantomData,
            }),
        );
        Ok(())
    }

    pub(crate) fn contains(&self, aggregate: TypeId) -> bool {
        self.by_aggregate.contains_key(&aggregate)
    }

    pub(crate) fn get<A: Aggregate>(&self) -> Option<Arc<dyn ErasedPolicy>> {
        self.by_aggregate.get(&TypeId::of::<A>()).cloned()
    }
}

/// Declared subjections that boot must find honoured.
///
/// A slice that owns an aggregate a *later* slice must guard cannot write the
/// guard's call site — the later slice does not exist yet. So it declares the
/// requirement here instead, and [`PostSaveSeams::verify`] turns an unhonoured
/// declaration into a loud boot failure, the way the schema type gate turns an
/// undeclared root field into one. A missing seam then has a positive form the
/// compiler and the operator can see, rather than being the silent absence of a
/// call.
#[derive(Default)]
pub(crate) struct PostSaveSeams {
    required: Vec<(TypeId, &'static str)>,
}

impl PostSaveSeams {
    pub(crate) fn require<A: Aggregate>(&mut self) {
        let type_id = TypeId::of::<A>();
        if !self.required.iter().any(|(id, _)| *id == type_id) {
            self.required.push((type_id, std::any::type_name::<A>()));
        }
    }

    pub(crate) fn verify(&self, policies: &PostSavePolicies) -> Result<(), EngineError> {
        for (type_id, aggregate) in &self.required {
            if !policies.contains(*type_id) {
                return Err(EngineError::UnhonouredSeam { aggregate });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{Persistence, PersistenceStyle};
    use futures_util::future::BoxFuture;
    use sqlx::PgConnection;

    struct Alpha;
    struct AlphaStore;
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
        let mut policies = PostSavePolicies::default();
        policies
            .register::<Alpha, _>(|_a, _ps| Ok(()))
            .expect("first policy registers");
        let again = policies.register::<Alpha, _>(|_a, _ps| Ok(()));
        assert!(matches!(again, Err(EngineError::Config(_))));
    }

    #[test]
    fn an_unhonoured_subjection_fails_verification_naming_the_aggregate() {
        let mut seams = PostSaveSeams::default();
        seams.require::<Alpha>();
        let policies = PostSavePolicies::default();
        let error = seams.verify(&policies).unwrap_err();
        assert!(
            matches!(error, EngineError::UnhonouredSeam { aggregate } if aggregate.contains("Alpha"))
        );
    }

    #[test]
    fn a_subjection_honoured_by_a_registered_policy_verifies() {
        let mut seams = PostSaveSeams::default();
        seams.require::<Alpha>();
        seams.require::<Alpha>(); // idempotent
        let mut policies = PostSavePolicies::default();
        policies.register::<Alpha, _>(|_a, _ps| Ok(())).unwrap();
        assert!(seams.verify(&policies).is_ok());
    }

    #[test]
    fn a_policy_for_a_different_aggregate_does_not_honour_the_seam() {
        let mut seams = PostSaveSeams::default();
        seams.require::<Alpha>();
        let mut policies = PostSavePolicies::default();
        policies.register::<Beta, _>(|_b, _ps| Ok(())).unwrap();
        assert!(seams.verify(&policies).is_err());
    }
}
