use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, render_config, runtime, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::registry::RenderRegistry;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(3);

#[tokio::test]
async fn s13_a_client_dropped_mid_outage_is_reaped_before_the_beat_spends_a_repair_on_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let switch = Arc::new(AtomicBool::new(false));

    let mut registry = RenderRegistry::new();
    registry.bind_noun::<Assignment>();
    registry.register_rls(SampleRls);
    registry.register_principal_resolver(SamplePrincipalResolver);
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_fail_switch(switch.clone()))
        .expect("register the spy projector");

    let render = runtime(&pool, render_config("pod-reap"), registry);

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    switch.store(true, Ordering::Relaxed);
    render
        .resnapshot_all()
        .await
        .expect("resnapshot_all returns Ok even when a per-session resnapshot fails");
    assert_eq!(
        render.live_sessions().await,
        1,
        "the session is held pending a repair"
    );

    spy.reset();
    switch.store(false, Ordering::Relaxed);
    drop(stream);

    let repaired = render.retry_repairs().await.expect("the repair pass runs");

    assert_eq!(
        (spy.populates(), spy.loads(), spy.projects()),
        (0, 0, 0),
        "a client that dropped mid-outage is reaped before the beat, so no populate, load or \
         project is spent re-snapshotting it into a closed stream"
    );
    assert_eq!(repaired, 0, "the reaped session is not counted as a repair");
    assert_eq!(
        render.live_sessions().await,
        0,
        "the dropped session is gone, not merely idle"
    );

    db.cleanup().await;
}
