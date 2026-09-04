use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use br_util_axum_readiness::ReadinessHandle;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use conformance_service_engine::sample::titles::{MiskeyedProjector, TitleProjector};
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::name::ProjectorName;
use service_engine::wire::Noun;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s21_noun_fanout() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let titles = Arc::new(AtomicUsize::new(0));
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the first projector on the noun registers");
    registry
        .register_projector(TitleProjector::new(titles.clone()))
        .expect("a second projector on the same noun registers");
    let engine = runtime(&pool, render_config("pod-fanout"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![
                window(SpyAssignments::NAME, false),
                window(TitleProjector::NAME, false),
            ],
        ))
        .await
        .expect("the session attaches to both projectors on the noun");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    retitle(&pool, subject, "alpha renamed").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.loads, 2,
        "one impact on a noun rendered by two projectors loads both"
    );
    assert_eq!(
        report.deltas, 2,
        "one impact on a noun rendered by two projectors re-renders both"
    );
    let mut seen: Vec<ProjectorName> = Vec::new();
    for _ in 0..2 {
        let delta = next_delta(&mut stream, SOON).await.expect("an Upsert");
        seen.push(upserted(&delta).projector.clone());
    }
    seen.sort();
    assert_eq!(seen, vec![TitleProjector::NAME, SpyAssignments::NAME]);

    db.cleanup().await;
}

#[tokio::test]
async fn s21_a_projector_whose_key_does_not_decode_the_noun_key_is_refused_at_registration() {
    let mut registry = registry();
    let refusal = registry
        .register_projector(MiskeyedProjector)
        .expect_err("a projector keyed apart from its noun is refused");
    assert!(
        matches!(
            refusal,
            EngineError::NounKeyMismatch { ref projector, ref noun }
                if projector == &MiskeyedProjector::NAME && noun == &Assignment::NAME
        ),
        "the refusal names the projector and the noun, got {refusal:?}"
    );
    assert_eq!(registry.names().count(), 0, "nothing was registered");
}

#[tokio::test]
async fn s21_a_projector_rendering_a_noun_no_type_declares_is_refused_at_registration() {
    let mut registry = service_engine::registry::RenderRegistry::new();
    let refusal = registry
        .register_projector(SpyAssignments::new(Spy::new()))
        .expect_err("a projector on an unbound noun is refused");
    assert!(matches!(refusal, EngineError::UnboundNoun { .. }));
}

#[tokio::test]
async fn s21_the_engine_checks_projector_keys_against_the_bound_noun_not_the_first_registrant() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s21_noun_bind", "pod-s21"),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .bind_noun::<Assignment>()
        .expect("the assignment noun binds to its declared Uuid key");

    let refusal = engine
        .register_projector(MiskeyedProjector)
        .expect_err("the first projector on the noun is still refused when it is miskeyed");
    assert!(
        matches!(refusal, EngineError::NounKeyMismatch { .. }),
        "the key check is against the noun, not whichever projector registered first: {refusal:?}"
    );

    engine
        .register_projector(TitleProjector::new(std::sync::Arc::new(
            std::sync::atomic::AtomicUsize::new(0),
        )))
        .expect("a projector whose Uuid key decodes the noun's key registers");

    db.cleanup().await;
}
