#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::AttachVerifiedDoc;
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

const SHA256_HEX: &str = "35b1135247e25b36525c4f5bb88038cf310f05ab54aa450e406e9ef5c5fde911";

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

async fn present(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count");
    n > 0
}

#[tokio::test]
async fn s225_an_over_policy_verified_stage_is_refused_and_a_failed_row_is_reaped_with_its_object()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s225-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 64,
        orphan_after: Duration::from_millis(400),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s225",
        "pod-s225",
        minio.config(&bucket),
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

    let over_id = Uuid::now_v7();
    let over = executor
        .run::<AttachVerifiedDoc>(
            principal.clone(),
            AttachVerifiedDoc {
                id: over_id,
                tenant,
                name: "huge.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: 1_000_000,
                sha256_hex: SHA256_HEX.to_string(),
            },
        )
        .await;
    assert!(
        over.is_err(),
        "a verified size over max_bytes is refused at stage before any presign",
    );
    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.blob b JOIN sample_doc d ON d.blob_ref = b.id \
         WHERE d.id = $1",
    )
    .bind(over_id)
    .fetch_one(&pool)
    .await
    .expect("count staged rows for the over-policy attempt");
    assert_eq!(
        staged, 0,
        "the ceiling refuses before any presign, so no blob row and no doc row are staged",
    );

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "leaked.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits");
    let reference = reference_of(&pool, doc).await;
    let object_key: String =
        sqlx::query_scalar("SELECT object_key FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&pool)
            .await
            .expect("read object key");
    let status = post_upload(&http, upload.into_inner(), b"leaked-bytes".to_vec()).await;
    assert!(status.is_success(), "the object lands in the bucket");
    assert!(
        minio.object_exists(&bucket, &object_key).await,
        "the object is present before the failed row is reaped",
    );

    sqlx::query(
        "UPDATE service_engine.blob \
         SET state = 'failed', failed_reason = 'policy:SEEDED', failed_at = now() - interval '1 hour' \
         WHERE id = $1",
    )
    .bind(reference)
    .execute(&pool)
    .await
    .expect("seed the failed row");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let gone =
            !present(&pool, reference).await && !minio.object_exists(&bucket, &object_key).await;
        if gone {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the failed row was not reaped"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
