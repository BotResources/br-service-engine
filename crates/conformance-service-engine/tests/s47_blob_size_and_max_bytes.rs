#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot};
use sqlx::PgPool;
use uuid::Uuid;

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

async fn size_and_state(pool: &PgPool, id: Uuid) -> Option<(Option<i64>, String)> {
    sqlx::query_as("SELECT size, state FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read the blob reference row")
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

async fn upload(client: &reqwest::Client, url: String, bytes: Vec<u8>) {
    let put = client
        .put(url)
        .body(bytes)
        .send()
        .await
        .expect("upload the bytes to the presigned URL");
    assert!(put.status().is_success(), "MinIO accepted the upload");
}

#[tokio::test]
async fn s47_promotion_records_the_object_size_and_an_upload_past_max_bytes_is_reaped() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s47-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 16,
        orphan_after: Duration::from_millis(300),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s47",
        "pod-s47",
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

    let small = Uuid::now_v7();
    let small_url: OneShot<String> = executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: small,
                tenant,
                name: "small.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach the within-cap upload");
    let small_ref = reference_of(&pool, small).await;
    let within_cap = b"hello".to_vec();
    upload(&http, small_url.into_inner(), within_cap.clone()).await;

    let big = Uuid::now_v7();
    let big_url: OneShot<String> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: big,
                tenant,
                name: "big.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach the over-cap upload");
    let big_ref = reference_of(&pool, big).await;
    upload(&http, big_url.into_inner(), vec![0u8; 40]).await;

    wait_until(async || matches!(size_and_state(&pool, small_ref).await, Some((Some(_), _)))).await;
    let (size, state) = size_and_state(&pool, small_ref)
        .await
        .expect("the within-cap reference is still present after promotion");
    assert_eq!(
        size,
        Some(within_cap.len() as i64),
        "promotion records the object's byte size",
    );
    assert_eq!(state, "uploaded", "the within-cap upload is promoted");

    wait_until(async || size_and_state(&pool, big_ref).await.is_none()).await;
    let big_key = format!("blob/attachment/{big_ref}");
    assert!(
        !minio.object_exists(&bucket, &big_key).await,
        "the over-cap object is deleted from storage by the reaper",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
