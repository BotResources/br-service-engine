use std::time::Duration;

use conformance_service_engine::inbound_support::{dead_letter_rows, wait_for_dead_letter_rows};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    CreateWidget, create_widget_coords, publish_command, wait_for_widget, widget_count,
};
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use uuid::Uuid;

#[tokio::test]
async fn s76_a_reaction_whose_disposition_is_terminal_dead_letters_through_the_running_loop() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s76", "pod-s76").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pipeline engine never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let widget = Uuid::now_v7();
    let first = CreateWidget {
        id: widget,
        tenant,
        label: "first".to_string(),
    };
    publish_command(&fabric, &create_widget_coords(), Uuid::now_v7(), &first).await;
    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the first create commits the widget through the pipeline"
    );

    let duplicate = CreateWidget {
        id: widget,
        tenant,
        label: "second".to_string(),
    };
    publish_command(&fabric, &create_widget_coords(), Uuid::now_v7(), &duplicate).await;

    assert_eq!(
        wait_for_dead_letter_rows(&pool, 1).await,
        1,
        "a reaction whose insert violates the primary key returns Disposition::Terminal, \
         and the running inbound loop routes it to the dead-letter table itself, \
         never through a direct DeadLetters::record"
    );

    let dead_letters = DeadLetters::new(pool.clone());
    let listed = dead_letters
        .list(Some(DeadLetterSource::Reaction), 16)
        .await
        .expect("the ops view lists the terminal reaction by source");
    assert_eq!(
        listed.len(),
        1,
        "exactly one terminal reaction dead-letters"
    );
    assert_eq!(listed[0].source, "reaction");

    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "the terminal reaction rolled its transaction back and committed no second effect"
    );
    assert_eq!(
        dead_letter_rows(&pool).await,
        1,
        "the terminal disposition produced one dead letter and no retry storm"
    );

    shutdown.notify_one();
    running
        .await
        .expect("join the engine task")
        .expect("run ok");
    drop(nats);
    db.cleanup().await;
}
