#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::{AttachDoc, RepointDoc};
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
async fn s65_repointing_a_row_releases_the_old_blob_in_the_same_transaction() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s59-{}", Uuid::now_v7().simple());
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
        "se_s59",
        "pod-s59",
        minio.config(&bucket),
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
    let first: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: doc,
                tenant,
                name: "first.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach the first blob");
    let old_ref = reference_of(&pool, doc).await;
    let status = post_upload(&http, first.into_inner(), b"first-bytes".to_vec()).await;
    assert!(status.is_success(), "the first object lands");
    let old_key = format!("blob/attachment/{old_ref}");
    assert!(minio.object_exists(&bucket, &old_key).await);

    let second: OneShot<UploadUrl> = executor
        .run::<RepointDoc>(
            principal,
            RepointDoc {
                id: doc,
                name: "second.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
            },
        )
        .await
        .expect("repoint the doc at a fresh blob");

    let new_ref = reference_of(&pool, doc).await;
    assert_ne!(new_ref, old_ref, "the row points at the new reference");
    assert_eq!(
        state_of(&pool, old_ref).await.as_deref(),
        Some("orphaned"),
        "the dropped reference is released in the same transaction as the repoint",
    );
    let status = post_upload(&http, second.into_inner(), b"second-bytes".to_vec()).await;
    assert!(status.is_success(), "the second object lands");

    wait_until(async || state_of(&pool, old_ref).await.is_none()).await;
    assert!(
        !minio.object_exists(&bucket, &old_key).await,
        "the reaper deletes the released object from storage",
    );
    wait_until(async || state_of(&pool, new_ref).await.as_deref() == Some("uploaded")).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
