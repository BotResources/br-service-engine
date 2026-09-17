use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::widget_count;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{MintSecret, boot_pipeline_engine};
use uuid::Uuid;

#[tokio::test]
async fn s200_a_second_create_under_a_held_key_is_refused_with_key_reused() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s200a", "pod-s200a").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal.clone(),
            MintSecret {
                id,
                label: "first".to_string(),
                tenant,
            },
        )
        .await
        .expect("the first create commits under the creator-generated id");

    let refused = executor
        .run::<MintSecret>(
            principal.clone(),
            MintSecret {
                id,
                label: "second".to_string(),
                tenant,
            },
        )
        .await
        .expect_err("a create under an already-held key is refused");
    assert_eq!(
        refused.code(),
        Some("KEY_REUSED"),
        "the refusal carries the stable KEY_REUSED code"
    );
    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "the refused create wrote no second row and did not overwrite the first"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s200_two_concurrent_creates_under_one_key_leave_exactly_one_winner() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let first = member(&pool, Uuid::now_v7(), tenant).await;
    let second = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s200b", "pod-s200b").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    let (left, right) = tokio::join!(
        executor.run::<MintSecret>(
            first,
            MintSecret {
                id,
                label: "left".to_string(),
                tenant,
            },
        ),
        executor.run::<MintSecret>(
            second,
            MintSecret {
                id,
                label: "right".to_string(),
                tenant,
            },
        ),
    );

    assert_eq!(
        [left.is_ok(), right.is_ok()]
            .into_iter()
            .filter(|ok| *ok)
            .count(),
        1,
        "the advisory lock lets exactly one concurrent create win"
    );
    let loser = left.err().or(right.err()).expect("one create is refused");
    assert_eq!(
        loser.code(),
        Some("KEY_REUSED"),
        "the losing create is refused with KEY_REUSED, not a raw unique-violation error"
    );
    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "exactly one row exists after the race"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
