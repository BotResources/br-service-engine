use std::sync::Arc;

use conformance_service_engine::inbound_support::{
    SampleCommand, claim_rows, dead_letter_rows, publish, sample_handler, stub_with_effect,
    wait_for_effect_rows,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_render_engine;
use service_engine::inbound::{Dispatch, StubPayload};
use uuid::Uuid;

#[tokio::test]
async fn s070_two_pods_on_one_durable_process_each_message_exactly_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let mut engine =
        boot_render_engine(&db, nats.nats().await, "se_inbound_s070", "se-s070-a").await;
    engine
        .register_reaction::<SampleCommand, _, _>("shared-work", sample_handler)
        .expect("register_reaction records the reaction and derives its subscription");
    let subscriptions = engine.inbound_subscriptions();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].reaction, "shared-work");

    let stub_a = stub_with_effect(db.app_pool().clone());
    let stub_b = stub_with_effect(db.app_pool().clone()).with_counters(stub_a.counters());
    let counters = stub_a.counters();
    let dispatch_a: Arc<dyn Dispatch> = Arc::new(stub_a);
    let dispatch_b: Arc<dyn Dispatch> = Arc::new(stub_b);
    let loop_a = conformance_service_engine::inbound_support::start_loop(
        &nats.nats().await,
        subscriptions.clone(),
        dispatch_a,
        db.app_pool().clone(),
    )
    .await;
    let loop_b = conformance_service_engine::inbound_support::start_loop(
        &nats.nats().await,
        subscriptions,
        dispatch_b,
        db.app_pool().clone(),
    )
    .await;

    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
    for id in &ids {
        publish(&nats.nats().await, *id, &StubPayload::commit(), None).await;
    }
    assert_eq!(
        wait_for_effect_rows(db.app_pool(), 3).await,
        3,
        "one durable shared by two pods delivers each message to exactly one pod"
    );

    assert_eq!(
        counters.invocations(),
        3,
        "a shared durable dispatches each of the three messages once; per-pod \
         (non-competing) consumers would each receive all three and dispatch six"
    );
    assert_eq!(
        counters.noops(),
        0,
        "no message reaches a second pod, so no dispatch resolves to a duplicate or stale no-op"
    );
    for id in &ids {
        assert_eq!(claim_rows(db.app_pool(), *id).await, 1);
    }
    assert_eq!(dead_letter_rows(db.app_pool()).await, 0);

    loop_a.stop();
    loop_b.stop();
    loop_a.join().await;
    loop_b.join().await;
    db.cleanup().await;
}
