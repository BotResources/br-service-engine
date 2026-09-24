use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::gate::Gate;
use conformance_service_engine::sample::note::Note;
use conformance_service_engine::sample::principal::{
    FailingPrincipalResolver, SamplePrincipal, SamplePrincipalResolver, SampleRls,
};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use futures_util::future::BoxFuture;
use service_engine::error::{AttachError, EngineError};
use service_engine::impact::{Deps, Impact};
use service_engine::principal::{Principal, PrincipalResolver};
use service_engine::registry::RenderRegistry;
use service_engine::runtime::SessionRuntime;
use sqlx::PgPool;
use uuid::Uuid;

fn unreachable_fact<'a>(
    _pg: &'a PgPool,
    _principal: &'a mut SamplePrincipal,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async {
        Err(EngineError::Service(
            "the fact store is unreachable for the duration of the test".into(),
        ))
    })
}

fn gated_registry<R: PrincipalResolver<SamplePrincipal>>(
    resolver: R,
    gate: Arc<Gate>,
) -> RenderRegistry<SamplePrincipal> {
    let mut registry = RenderRegistry::new();
    registry.bind_noun::<Assignment>();
    registry.bind_noun::<Note>();
    registry.register_rls(SampleRls);
    registry.register_principal_resolver(resolver);
    registry
        .register_projector(SpyAssignments::new(Spy::new()).gated(gate))
        .expect("the gated projector registers on a bound noun");
    registry
}

async fn attach_across_a_refresh(
    engine: Arc<SessionRuntime<SamplePrincipal>>,
    gate: Arc<Gate>,
    principal: SamplePrincipal,
) -> Result<(), AttachError> {
    let attaching = {
        let engine = engine.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { engine.attach(request).await })
    };
    gate.wait_until_inside().await;
    let report = engine
        .render(vec![Impact::principal_facts(principal.id(), Deps::EMPTY)])
        .await
        .expect("the refresh pass runs while the session is still connecting");
    assert_eq!(report.ended, 1, "the refresh ends the connecting session");
    gate.release();
    let outcome = attaching.await.expect("the attach task joins").map(drop);
    assert_eq!(
        engine.live_sessions().await + engine.pending_sessions().await,
        0,
        "the ended attach leaves no session behind"
    );
    outcome
}

#[tokio::test]
async fn s258_an_attach_whose_principal_resolver_faults_is_internal_never_revoked() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let gate = Gate::new();
    let engine = runtime(
        &pool,
        render_config("pod-attach-resolver-fault"),
        gated_registry(FailingPrincipalResolver, gate.clone()),
    );

    let outcome = attach_across_a_refresh(engine, gate, principal).await;
    assert!(
        matches!(outcome, Err(AttachError::PrincipalRefreshFailed)),
        "a resolver fault is an infrastructure fault, answered INTERNAL, never the \
         UNAUTHENTICATED of a principal that no longer exists, got {outcome:?}"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s258_an_attach_whose_principal_fact_reload_faults_is_internal_never_revoked() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let gate = Gate::new();
    let mut registry = gated_registry(SamplePrincipalResolver, gate.clone());
    registry.register_principal_fact(unreachable_fact);
    let engine = runtime(&pool, render_config("pod-attach-fact-fault"), registry);

    let outcome = attach_across_a_refresh(engine, gate, principal).await;
    assert!(
        matches!(outcome, Err(AttachError::PrincipalRefreshFailed)),
        "a fact loader fault is an infrastructure fault, answered INTERNAL, never the \
         UNAUTHENTICATED of a principal that no longer exists, got {outcome:?}"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s258_an_attach_whose_principal_no_longer_exists_is_revoked() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let gate = Gate::new();
    let engine = runtime(
        &pool,
        render_config("pod-attach-revoked"),
        gated_registry(SamplePrincipalResolver, gate.clone()),
    );
    forget_member(&pool, principal.id().as_uuid()).await;

    let outcome = attach_across_a_refresh(engine, gate, principal).await;
    assert!(
        matches!(outcome, Err(AttachError::PrincipalRevoked)),
        "only a resolver answering that the principal no longer exists revokes the attach, \
         got {outcome:?}"
    );

    db.cleanup().await;
}
