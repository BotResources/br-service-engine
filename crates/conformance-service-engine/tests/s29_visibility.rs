#[allow(dead_code)]
mod engine_twin;

use std::collections::BTreeSet;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::gated::load_candidates;
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentVisibility, GatedAssignmentProjector};
use engine_twin::{SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Impact};
use service_engine::population::Population;
use service_engine::principal::Principal;
use service_engine::projector::Projector;
use service_engine::session::WindowParams;
use service_engine::visibility::check_window_matches_visibility;
use uuid::Uuid;

#[tokio::test]
async fn s29_engine_a_row_outside_the_visibility_is_filtered_from_the_snapshot() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = assignment(&pool, home, "mine").await;
    let theirs = assignment(&pool, foreign, "theirs").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config("se_s29_filter", "pod-s29a"),
        GatedAssignmentProjector::all(),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(GatedAssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the gated session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let visible = assignment_ids(reset_views(&reset));
    assert_eq!(
        visible,
        vec![mine],
        "the query returns only rows whose cohorts meet the principal's memberships"
    );
    assert!(
        !visible.contains(&theirs),
        "a row in another tenant is never in the snapshot, though the window populated every key"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s29_the_session_window_membership_is_derived_from_the_visibility_declaration() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let principal = SamplePrincipal::new(Uuid::now_v7(), home);
    let mine = assignment(&pool, home, "mine").await;
    let theirs = assignment(&pool, foreign, "theirs").await;

    let projector = GatedAssignmentProjector::membership();
    let population = projector
        .populate(&pool, &WindowParams::none(), &principal)
        .await
        .expect("the window populates");
    let produced: BTreeSet<Uuid> = match population {
        Population::Keys(keys) => keys,
        Population::Ordered { keys, .. } => keys.into_iter().collect(),
        Population::Query(query) => query.keys().clone(),
    };

    let candidates = load_candidates(&pool)
        .await
        .expect("the candidate rows load");
    check_window_matches_visibility::<AssignmentVisibility, _, _>(
        &produced, candidates, &principal,
    )
    .expect("the window the projector populated equals the visibility declaration");

    assert_eq!(
        produced,
        BTreeSet::from([mine]),
        "the window holds exactly the keys whose cohorts meet the principal's memberships"
    );
    assert!(
        !produced.contains(&theirs),
        "a foreign-tenant row is excluded by the window itself, not only by the render filter"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s29_engine_losing_the_membership_removes_the_row_from_the_live_session() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let exile = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config("se_s29_revoke", "pod-s29b"),
        GatedAssignmentProjector::membership(),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(GatedAssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the gated session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![subject]);

    move_member(&pool, principal.id().as_uuid(), exile).await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )],
    )
    .await;

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("losing the membership removes the row that is no longer visible");
    assert!(matches!(delta, Delta::Remove { .. }));
    assert_eq!(removed_key(&delta), subject);
    assert_eq!(delta.revision().get(), 2);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
