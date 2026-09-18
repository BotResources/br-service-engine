use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{MintSecret, widget_label};
use service_engine::OneShot;
use uuid::Uuid;

#[tokio::test]
async fn s087_a_one_shot_secret_is_returned_only_on_the_synchronous_channel() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s087", "pod-s087").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let principal =
        conformance_service_engine::sample::render::member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let widget = Uuid::now_v7();
    let secret: OneShot<String> = executor
        .run::<MintSecret>(
            principal,
            MintSecret {
                id: widget,
                label: "vault".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint mutation commits and returns its one-shot on the sync channel");

    let secret = secret.into_inner();
    assert_eq!(secret, format!("secret-for-{widget}"));

    let outbox: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.integration_outbox")
        .fetch_one(&pool)
        .await
        .expect("count outbox rows");
    assert_eq!(outbox, 0, "a one-shot secret never becomes an outbox row");

    let stored = widget_label(&pool, widget)
        .await
        .expect("the widget was committed");
    assert!(
        !stored.contains(&secret),
        "the committed state holds no copy of the one-shot secret",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
