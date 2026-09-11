use std::time::Duration;

use conformance_service_engine::inbound_support::{dead_letter_rows, wait_for_dead_letter_rows};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_panic_engine;
use conformance_service_engine::sample::pipeline::{
    DetonateWidget, detonate_widget_coords, publish_command, wait_for_widget,
};
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use uuid::Uuid;

#[tokio::test]
async fn s173_a_panicking_reaction_dead_letters_once_and_the_durable_keeps_serving() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();

    let engine = boot_panic_engine(&db, fabric.clone(), "se_s173", "pod-s173").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the panic engine never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let poison = DetonateWidget {
        id: Uuid::now_v7(),
        tenant,
        label: "boom".to_string(),
        explode: true,
    };
    publish_command(&fabric, &detonate_widget_coords(), Uuid::now_v7(), &poison).await;

    assert_eq!(
        wait_for_dead_letter_rows(&pool, 1).await,
        1,
        "a reaction handler that panics is caught and routed to the dead-letter table, not left \
         redelivering forever with readiness UP"
    );
    let listed = DeadLetters::new(pool.clone())
        .list(Some(DeadLetterSource::Reaction), 16)
        .await
        .expect("list the dead letter by source");
    assert_eq!(listed.len(), 1, "exactly one panicking frame dead-letters");
    assert_eq!(listed[0].source, "reaction");

    let good = DetonateWidget {
        id: Uuid::now_v7(),
        tenant,
        label: "safe".to_string(),
        explode: false,
    };
    publish_command(&fabric, &detonate_widget_coords(), Uuid::now_v7(), &good).await;
    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the same durable keeps serving after the panic: the next, non-panicking command commits"
    );

    assert_eq!(
        dead_letter_rows(&pool).await,
        1,
        "the panic produced one dead letter and no redelivery storm"
    );
    assert_eq!(
        readiness.snapshot(),
        service_engine::Readiness::Ready,
        "a single caught panic does not lower readiness"
    );

    shutdown.notify_one();
    running
        .await
        .expect("join the engine task")
        .expect("run ok");
    drop(nats);
    db.cleanup().await;
}
