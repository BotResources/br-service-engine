#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::AttachVerifiedDoc;
use conformance_service_engine::sample::boot_blob_fault_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, Readiness, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

const PAYLOAD: &[u8] = b"verified-bytes";
const SHA256_HEX: &str = "35b1135247e25b36525c4f5bb88038cf310f05ab54aa450e406e9ef5c5fde911";

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

async fn state_of(pool: &PgPool, reference: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
        .bind(reference)
        .fetch_one(pool)
        .await
        .expect("read state")
}

#[tokio::test]
async fn s234_a_faulting_post_upload_policy_leaves_the_row_pending_and_the_pod_ready() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s234-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };

    let mut engine = boot_blob_fault_engine(
        &db,
        nats.nats().await,
        "se_s234",
        "pod-s234",
        minio.config(&bucket),
        policy,
        Duration::from_millis(150),
    )
    .await;
    let rounds = engine.observe_blob_reaper_rounds();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachVerifiedDoc>(
            principal,
            AttachVerifiedDoc {
                id: doc,
                tenant,
                name: "faulty.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: PAYLOAD.len() as u64,
                sha256_hex: SHA256_HEX.to_string(),
            },
        )
        .await
        .expect("the verified attach commits");
    let reference = reference_of(&pool, doc).await;

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(status.is_success(), "the exact bytes land: {status}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let faulted = rounds
            .lock()
            .unwrap()
            .iter()
            .any(|round| round.failures > 0);
        if faulted {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper never recorded a failed sweep against the faulting policy",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        state_of(&pool, reference).await.as_deref(),
        Some("pending"),
        "a deterministic policy fault rolls the promotion back, so the row stays pending, never \
         promoted and never failed",
    );
    assert_eq!(
        readiness.snapshot(),
        Readiness::Ready,
        "a faulting policy retries on the next sweep; it never takes the pod out of rotation",
    );
    assert!(
        rounds
            .lock()
            .unwrap()
            .iter()
            .all(|round| round.promoted == 0),
        "no sweep promoted the row while the policy keeps faulting",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
