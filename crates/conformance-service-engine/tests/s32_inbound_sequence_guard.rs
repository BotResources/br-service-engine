use std::sync::Arc;

use conformance_service_engine::inbound_support::{
    dead_letter_rows, effect_present, publish, sample_subscription, settle, stub_with_effect,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{Dispatch, StubPayload};
use uuid::Uuid;

#[tokio::test]
async fn s32_a_stale_producer_sequence_is_a_no_op_and_never_walks_the_view_backwards() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let dispatch: Arc<dyn Dispatch> = Arc::new(stub_with_effect(db.app_pool().clone()));
    let inbound = start(&db, &nats, dispatch).await;

    let ahead = Uuid::now_v7();
    let stale = Uuid::now_v7();
    let newer = Uuid::now_v7();

    publish(
        &nats.nats().await,
        ahead,
        &StubPayload::commit(),
        Some(("runner", "thread-1", 2)),
    )
    .await;
    settle().await;
    publish(
        &nats.nats().await,
        stale,
        &StubPayload::commit(),
        Some(("runner", "thread-1", 1)),
    )
    .await;
    settle().await;
    publish(
        &nats.nats().await,
        newer,
        &StubPayload::commit(),
        Some(("runner", "thread-1", 3)),
    )
    .await;
    settle().await;

    assert_eq!(
        effect_present(db.app_pool(), ahead).await,
        1,
        "sequence 2 applies"
    );
    assert_eq!(
        effect_present(db.app_pool(), stale).await,
        0,
        "sequence 1 is below the last applied 2 and is dropped as an acked no-op"
    );
    assert_eq!(
        effect_present(db.app_pool(), newer).await,
        1,
        "sequence 3 applies"
    );

    let last: i64 = sqlx::query_scalar(
        "SELECT last_seq FROM service_engine.sequence_guard WHERE producer = $1 AND seq_key = $2",
    )
    .bind("runner")
    .bind("thread-1")
    .fetch_one(db.app_pool())
    .await
    .expect("read the sequence guard");
    assert_eq!(last, 3, "the guard holds the highest applied sequence");
    assert_eq!(
        dead_letter_rows(db.app_pool()).await,
        0,
        "a stale sequence is a no-op, never a poison"
    );

    inbound.stop();
    inbound.join().await;
    db.cleanup().await;
}

async fn start(
    db: &TestDb,
    nats: &TestNats,
    dispatch: Arc<dyn Dispatch>,
) -> service_engine::inbound::InboundLoop {
    conformance_service_engine::inbound_support::start_loop(
        &nats.nats().await,
        vec![sample_subscription("sequence-work")],
        dispatch,
        db.app_pool().clone(),
    )
    .await
}
