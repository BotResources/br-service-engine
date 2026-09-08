use std::sync::Arc;

use conformance_service_engine::inbound_support::{
    dead_letter_rows, effect_rows, publish, sample_subscription, start_loop, stub_with_effect,
    wait_for_dead_letter_rows,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{
    DeadLetterSource, DeadLetters, DiscardOutcome, Dispatch, StubPayload, StubVerdict,
};
use uuid::Uuid;

#[tokio::test]
async fn s30_a_terminal_message_dead_letters_and_discard_removes_it() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let dispatch: Arc<dyn Dispatch> = Arc::new(stub_with_effect(db.app_pool().clone()));
    let inbound = start_loop(
        &nats.nats().await,
        vec![sample_subscription("poison-work")],
        dispatch,
        db.app_pool().clone(),
    )
    .await;

    let id = Uuid::now_v7();
    publish(
        &nats.nats().await,
        id,
        &StubPayload::of(StubVerdict::Terminal),
        None,
    )
    .await;

    assert_eq!(
        wait_for_dead_letter_rows(db.app_pool(), 1).await,
        1,
        "a poison message lands in the dead-letter table instead of blocking the consumer"
    );
    assert_eq!(
        effect_rows(db.app_pool()).await,
        0,
        "a poison message never commits an effect"
    );

    let dead_letters = DeadLetters::new(db.app_pool().clone());
    let listed = dead_letters
        .list(Some(DeadLetterSource::Reaction), 16)
        .await
        .expect("the ops view lists the dead letters by source");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].message_id, id);
    assert_eq!(listed[0].source, "reaction");

    let discarded = dead_letters
        .discard(listed[0].id)
        .await
        .expect("discard is a mutation on the ops view");
    assert_eq!(discarded, DiscardOutcome::Discarded);
    assert_eq!(dead_letter_rows(db.app_pool()).await, 0);

    inbound.stop();
    inbound.join().await;
    db.cleanup().await;
}
