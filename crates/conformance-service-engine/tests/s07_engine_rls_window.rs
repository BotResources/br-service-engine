#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SOON, await_ready, spy_engine};
use uuid::Uuid;

const CHANNEL: &str = "se_s07_engine";

#[tokio::test]
async fn s07_engine_an_rls_window_loads_only_the_principals_own_rows() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = assignment(&pool, home, "alpha").await;
    let theirs = assignment(&pool, elsewhere, "beta").await;

    let guarded = Spy::new();
    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s07"),
        SpyAssignments::new(guarded.clone()),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect("the RLS session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the RLS session opens with its Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![mine]);

    assert!(
        guarded.ever_loaded(mine),
        "the principal's own row is in Facts"
    );
    assert!(
        !guarded.ever_loaded(theirs),
        "a row invisible to the principal is never in Facts: the PerPrincipal load ran inside a \
         transaction the registered RlsApplier prepared"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
