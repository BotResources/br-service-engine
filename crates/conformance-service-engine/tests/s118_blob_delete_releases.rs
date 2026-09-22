#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::{AttachDoc, DeleteDoc};
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

async fn state_of(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read the blob reference state")
}

async fn doc_exists(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM sample_doc WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count docs");
    n > 0
}

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the doc's blob reference")
}

async fn wait_until<F>(mut check: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper did not converge within the deadline",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn s118_deleting_the_aggregate_releases_its_blobs() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s60-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_millis(300),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s60",
        "pod-s60",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(300)),
        policy,
        Duration::from_millis(100),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let http = reqwest::Client::new();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: doc,
                tenant,
                name: "doomed.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach a blob to the doc");
    let reference = reference_of(&pool, doc).await;
    let status = post_upload(&http, upload.into_inner(), b"doomed-bytes".to_vec()).await;
    assert!(status.is_success(), "the object lands before deletion");
    let object_key = format!("blob/attachment/{reference}");
    assert!(minio.object_exists(&bucket, &object_key).await);

    executor
        .run::<DeleteDoc>(principal, DeleteDoc { id: doc })
        .await
        .expect("delete the aggregate");

    assert!(!doc_exists(&pool, doc).await, "the aggregate row is gone");
    assert_eq!(
        state_of(&pool, reference).await.as_deref(),
        Some("orphaned"),
        "deleting the aggregate releases its blob in the same transaction",
    );

    wait_until(async || state_of(&pool, reference).await.is_none()).await;
    assert!(
        !minio.object_exists(&bucket, &object_key).await,
        "the reaper deletes the released object from storage",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
