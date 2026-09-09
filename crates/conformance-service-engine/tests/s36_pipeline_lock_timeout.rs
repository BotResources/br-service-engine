use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    LockWidget, insert_widget, lock_widget_coords, publish_command, widget_label,
};
use sqlx::PgPool;
use uuid::Uuid;

async fn dead_letters(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter")
        .fetch_one(pool)
        .await
        .expect("count dead letters")
}

#[tokio::test]
async fn s36_a_row_lock_timeout_is_retryable_and_never_dead_letters() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "origin").await;

    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s36", "pod-s36").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(tokio::time::Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let mut holder = pool.begin().await.expect("open the holder transaction");
    sqlx::query("SELECT id FROM sample_widget WHERE id = $1 FOR UPDATE")
        .bind(widget)
        .execute(&mut *holder)
        .await
        .expect("take the row lock");

    publish_command(
        &fabric,
        &lock_widget_coords(),
        Uuid::now_v7(),
        &LockWidget {
            id: widget,
            label: "relabelled".to_string(),
        },
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        widget_label(&pool, widget).await.as_deref(),
        Some("origin"),
        "the contended write did not commit while the lock was held",
    );
    assert_eq!(
        dead_letters(&pool).await,
        0,
        "a lock timeout is retryable (nak), never terminal, so it does not dead-letter",
    );

    holder.rollback().await.expect("release the row lock");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if widget_label(&pool, widget).await.as_deref() == Some("relabelled") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retried delivery never committed after the lock was released",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(dead_letters(&pool).await, 0);

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
