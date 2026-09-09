#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::inbound_support::wait_for_dead_letter_rows;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_persistence_engine;
use conformance_service_engine::sample::bump_full_coords;
use conformance_service_engine::sample::pipeline::publish_command;
use engine_twin::await_ready;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use uuid::Uuid;

#[tokio::test]
async fn s097_an_integrity_violation_in_cx_save_is_terminal_whatever_the_handler_disposition_says()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_persistence_engine(&db, fabric.clone(), "se_s097", "pod-s097").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let key = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let command = serde_json::json!({
        "key": key, "tenant": tenant, "amount": 150, "author": "alice"
    });
    publish_command(&fabric, &bump_full_coords(), message_id, &command).await;

    assert_eq!(
        wait_for_dead_letter_rows(&pool, 1).await,
        1,
        "the class-23 ceiling violation dead-letters instead of naking forever under the \
         handler's coarse Retry disposition",
    );

    let dead_letters = DeadLetters::new(pool.clone());
    let listed = dead_letters
        .list(Some(DeadLetterSource::Reaction), 16)
        .await
        .expect("list the dead letters");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].message_id, message_id);
    assert_eq!(
        listed[0].delivered, 1,
        "the engine classified the integrity violation terminal on the first delivery, not after \
         exhausting the retry budget",
    );

    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_counter_full_snapshot WHERE id = $1")
            .bind(key)
            .fetch_one(&pool)
            .await
            .expect("count the snapshots");
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_counter_full_event WHERE counter = $1")
            .bind(key)
            .fetch_one(&pool)
            .await
            .expect("count the events");
    assert_eq!(snapshots, 0, "the rejected write committed nothing");
    assert_eq!(events, 0, "the rejected write's events committed nothing");

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
