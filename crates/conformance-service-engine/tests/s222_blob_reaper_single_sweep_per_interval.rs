#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

#[tokio::test]
async fn s222_two_pods_run_at_most_one_reaper_sweep_per_interval() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s222-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let interval = Duration::from_millis(200);

    let engine_a = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s222",
        "pod-s222a",
        minio.config(&bucket),
        policy,
        interval,
    )
    .await;
    let engine_b = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s222",
        "pod-s222b",
        minio.config(&bucket),
        policy,
        interval,
    )
    .await;
    let ready_a = engine_a.readiness();
    let ready_b = engine_b.readiness();
    let stop_a = engine_a.shutdown_handle();
    let stop_b = engine_b.shutdown_handle();
    let executor = engine_a.mutation_executor();
    let http = reqwest::Client::new();
    let run_a = tokio::spawn(engine_a.run());
    let run_b = tokio::spawn(engine_b.run());
    await_ready(&ready_a).await;
    await_ready(&ready_b).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "shared.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits");
    let reference = reference_of(&pool, doc).await;
    let status = post_upload(&http, upload.into_inner(), b"shared-bytes".to_vec()).await;
    assert!(status.is_success());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
                .bind(reference)
                .fetch_one(&pool)
                .await
                .expect("read state");
        if state.as_deref() == Some("uploaded") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the blob was never promoted"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;

    let slots: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.leader_slot WHERE name = 'reaper:blob'",
    )
    .fetch_one(&pool)
    .await
    .expect("count reaper slots");
    assert!(slots >= 1, "at least one reaper:blob slot ran");

    let contended: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ( \
           SELECT slot FROM service_engine.leader_slot \
           WHERE name = 'reaper:blob' GROUP BY slot HAVING count(DISTINCT pod) > 1 \
         ) contended",
    )
    .fetch_one(&pool)
    .await
    .expect("count contended slots");
    assert_eq!(
        contended, 0,
        "no reaper:blob slot was ever run by two pods in the same interval",
    );

    stop_a.notify_one();
    stop_b.notify_one();
    run_a.await.expect("join a").expect("a returns Ok");
    run_b.await.expect("join b").expect("b returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
