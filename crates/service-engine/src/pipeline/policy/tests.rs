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
    let error = seams
        .verify(&policies.save, &policies.delete, &policies.upload)
        .unwrap_err();
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
    let error = seams
        .verify(&policies.save, &policies.delete, &policies.upload)
        .unwrap_err();
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
    assert!(
        seams
            .verify(&policies.save, &policies.delete, &policies.upload)
            .is_ok()
    );
}
