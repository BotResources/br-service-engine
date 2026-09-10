use conformance_service_engine::infra::TestDb;
use service_engine::inbound::{Ordering, SequenceKey, advance_sequence};

fn key() -> SequenceKey {
    SequenceKey {
        producer: "orders".to_string(),
        key: "order-1".to_string(),
    }
}

#[tokio::test]
async fn s156_two_reactions_of_one_producer_keep_independent_watermarks() {
    let db = TestDb::fresh().await;
    let mut conn = db
        .app_pool()
        .acquire()
        .await
        .expect("a connection for the guard writes");

    let first = advance_sequence(&mut conn, "confirmed_reaction", &key(), 5)
        .await
        .expect("the first reaction advances its own watermark");
    assert_eq!(first, Ordering::Advanced);

    let other = advance_sequence(&mut conn, "shipped_reaction", &key(), 3)
        .await
        .expect("a second reaction on the same producer and key advances too");
    assert_eq!(
        other,
        Ordering::Advanced,
        "the guard is scoped per reaction, so a second reaction consuming a different fact of the \
         same producer under the same key is never dropped by the first reaction's watermark"
    );

    let regression = advance_sequence(&mut conn, "confirmed_reaction", &key(), 3)
        .await
        .expect("the guard query runs");
    assert_eq!(
        regression,
        Ordering::Stale,
        "within one reaction a sequence below the watermark is still stale, so ordering per \
         reaction holds"
    );

    drop(conn);
    db.cleanup().await;
}
