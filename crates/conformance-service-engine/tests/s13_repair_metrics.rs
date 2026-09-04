use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::metrics_probe;
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, render_config, runtime, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::metrics::RESETS_TOTAL;
use service_engine::registry::RenderRegistry;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(3);

#[tokio::test]
async fn s13_a_reconnect_reset_and_a_beat_repair_both_reach_the_resets_metric() {
    let probe = metrics_probe::install();

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
    let render = runtime(&pool, render_config("pod-repair-metric"), registry);

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    let before_reconnect = probe.total(RESETS_TOTAL);
    render
        .resnapshot_all()
        .await
        .expect("a clean reconnect re-snapshots every live session");
    let after_reconnect = probe.total(RESETS_TOTAL);
    assert!(
        after_reconnect > before_reconnect,
        "a reconnect Reset must reach service_engine_resets_total, not only the internal counter \
         (before={before_reconnect}, after={after_reconnect})"
    );

    switch.store(true, Ordering::Relaxed);
    render
        .resnapshot_all()
        .await
        .expect("resnapshot_all returns Ok while a per-session resnapshot fails");
    let before_beat = probe.total(RESETS_TOTAL);
    switch.store(false, Ordering::Relaxed);

    let repaired = render.retry_repairs().await.expect("the beat repair runs");
    assert!(
        repaired >= 1,
        "the pending session was repaired by the beat"
    );
    let after_beat = probe.total(RESETS_TOTAL);
    assert!(
        after_beat > before_beat,
        "a beat-driven repair Reset must reach service_engine_resets_total, not bypass it \
         (before={before_beat}, after={after_beat})"
    );

    db.cleanup().await;
}
