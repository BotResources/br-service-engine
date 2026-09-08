use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use conformance_service_engine::inbound_support::{
    dead_letter_rows, effect_present, effect_writer, publish, sample_command_coords,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{
    Budgets, DeadLetters, Dispatch, InboundConfig, InboundLoop, ReactionCoordinates, RetryOutcome,
    StubDispatch, StubPayload, StubVerdict, Subscription,
};
use uuid::Uuid;

#[tokio::test]
async fn s31_an_early_message_parks_then_lands_on_release_while_a_stuck_one_dead_letters() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let stub = StubDispatch::new(db.app_pool().clone()).with_effect(effect_writer());
    let gate = stub.release_gate();
    let dispatch: Arc<dyn Dispatch> = Arc::new(stub);
    let subscription = Subscription::new(
        "parking-work",
        ReactionCoordinates::Command(sample_command_coords()),
    )
    .with_budgets(Budgets {
        delivery: 8,
        parking: 3,
    });
    let inbound = InboundLoop::start(
        nats.nats().await,
        vec![subscription],
        dispatch,
        DeadLetters::new(db.app_pool().clone()),
        InboundConfig {
            ack_wait: Duration::from_secs(2),
            backoff_base: Duration::from_millis(60),
            backoff_max: Duration::from_millis(500),
            ..InboundConfig::default()
        },
    )
    .await
    .expect("the inbound loop starts");

    let early = Uuid::now_v7();
    let stuck = Uuid::now_v7();
    publish(
        &nats.nats().await,
        early,
        &StubPayload::of(StubVerdict::ParkUntilReleased),
        None,
    )
    .await;
    publish(
        &nats.nats().await,
        stuck,
        &StubPayload::of(StubVerdict::Park),
        None,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(
        effect_present(db.app_pool(), early).await,
        1,
        "an early message parks and lands once its aggregate is released"
    );
    assert_eq!(
        effect_present(db.app_pool(), stuck).await,
        0,
        "a message that never releases never commits an effect"
    );
    assert_eq!(
        dead_letter_rows(db.app_pool()).await,
        1,
        "a message parked past its parking budget dead-letters"
    );

    let dead_letters = DeadLetters::new(db.app_pool().clone());
    let listed = dead_letters
        .list(None, 16)
        .await
        .expect("list dead letters");
    assert_eq!(listed[0].message_id, stuck);
    let outcome = dead_letters
        .retry(listed[0].id, &nats.nats().await)
        .await
        .expect("retry re-enters the pipeline like a fresh delivery");
    assert_eq!(outcome, RetryOutcome::Republished);

    inbound.stop();
    inbound.join().await;
    db.cleanup().await;
}
