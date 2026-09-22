#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::{AttachDoc, DetachDoc};
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

async fn blob_present(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count blob references");
    n > 0
}

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
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
async fn s107_the_reaper_removes_an_abandoned_upload_and_an_orphan_past_the_bound() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s107-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_millis(400),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s107",
        "pod-s107",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(400)),
        policy,
        Duration::from_millis(150),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let http = reqwest::Client::new();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let abandoned = Uuid::now_v7();
    executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: abandoned,
                tenant,
                name: "never-uploaded.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach the abandoned upload");
    let abandoned_ref = reference_of(&pool, abandoned).await;

    let orphan = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: orphan,
                tenant,
                name: "orphan.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach the soon-to-be orphan");
    let orphan_ref = reference_of(&pool, orphan).await;
    let status = post_upload(&http, upload.into_inner(), b"orphan-bytes".to_vec()).await;
    assert!(status.is_success());
    let orphan_key = format!("blob/attachment/{orphan_ref}");
    assert!(
        minio.object_exists(&bucket, &orphan_key).await,
        "the orphan's object is present before it is released",
    );
    executor
        .run::<DetachDoc>(principal, DetachDoc { id: orphan })
        .await
        .expect("detach releases the blob, marking it an orphan");

    wait_until(async || !blob_present(&pool, abandoned_ref).await).await;
    wait_until(async || !blob_present(&pool, orphan_ref).await).await;
    assert!(
        !minio.object_exists(&bucket, &orphan_key).await,
        "the orphan's object is deleted from storage by the reaper",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
