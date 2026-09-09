use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{ScheduleCreate, wait_for_widget};
use uuid::Uuid;

#[tokio::test]
async fn s090_schedule_at_fires_on_the_db_clock_and_runs_through_the_pipeline() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s090", "pod-s090").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let principal =
        conformance_service_engine::sample::render::member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(tokio::time::Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let widget = Uuid::now_v7();
    executor
        .run::<ScheduleCreate>(
            principal,
            ScheduleCreate {
                id: widget,
                tenant,
                label: "scheduled".to_string(),
            },
        )
        .await
        .expect("the schedule mutation commits");

    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the scheduled reaction fired on the DB clock and ran through the pipeline",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
