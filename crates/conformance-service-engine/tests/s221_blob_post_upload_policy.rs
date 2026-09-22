#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::AttachVerifiedDoc;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    boot_blob_policy_engine, boot_unhonoured_upload_seam_engine,
};
use engine_twin::await_ready;
use service_engine::error::EngineError;
use service_engine::{BlobPolicy, OneShot, UploadUrl};
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

async fn outbox_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.integration_outbox \
         WHERE subject LIKE 'integration.evt.sample.attachment.uploaded%'",
    )
    .fetch_one(pool)
    .await
    .expect("count outbox rows")
}

async fn uploaded_event_metadata(pool: &PgPool) -> serde_json::Value {
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM service_engine.integration_outbox \
         WHERE subject LIKE 'integration.evt.sample.attachment.uploaded%' \
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("read the uploaded event payload");
    payload
}

async fn wait_state(pool: &PgPool, id: Uuid, want: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
                .bind(id)
                .fetch_optional(pool)
                .await
                .expect("read state")
                .flatten();
        if state.as_deref() == Some(want) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never reached {want}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn upload_verified(
    pool: &PgPool,
    executor: &service_engine::pipeline::MutationExecutor<
        conformance_service_engine::sample::principal::SamplePrincipal,
    >,
    principal: conformance_service_engine::sample::principal::SamplePrincipal,
    tenant: Uuid,
    http: &reqwest::Client,
) -> Uuid {
    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachVerifiedDoc>(
            principal,
            AttachVerifiedDoc {
                id: doc,
                tenant,
                name: "policy.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: PAYLOAD.len() as u64,
                sha256_hex: SHA256_HEX.to_string(),
            },
        )
        .await
        .expect("the verified attach commits");
    let reference = reference_of(pool, doc).await;
    let status = post_upload(http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(status.is_success(), "the exact bytes land");
    reference
}

#[tokio::test]
async fn s221_a_post_upload_policy_runs_in_the_promotion_transaction_or_fails_the_row() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s221-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();
    let http = reqwest::Client::new();
    let tenant = Uuid::now_v7();
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_millis(400),
    };

    let unhonoured = boot_unhonoured_upload_seam_engine(
        &db,
        nats.nats().await,
        "se_s221u",
        "pod-s221u",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(400)),
        policy,
    )
    .await;
    let error = unhonoured
        .run()
        .await
        .expect_err("run refuses the unhonoured seam");
    assert!(
        matches!(error, EngineError::UnhonouredSeam { aggregate } if aggregate.contains("Attachment")),
        "the boot error names the unhonoured blob kind: {error:?}",
    );

    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_blob_policy_engine(
        &db,
        nats.nats().await,
        "se_s221",
        "pod-s221",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(400)),
        policy,
        Duration::from_millis(150),
        false,
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reference = upload_verified(&pool, &executor, principal, tenant, &http).await;
    wait_state(&pool, reference, "uploaded").await;
    assert!(
        outbox_count(&pool).await >= 1,
        "the policy's emit committed to the outbox inside the promotion transaction",
    );

    let event = uploaded_event_metadata(&pool).await;
    let metadata = &event["metadata"];
    assert_eq!(
        metadata["actor_kind"], "service",
        "the reaper's emit carries the service actor, never the uploading human: {metadata}",
    );
    assert_eq!(
        metadata["correlation_id"],
        serde_json::Value::String(reference.to_string()),
        "the emit correlates on the blob id",
    );
    assert!(
        metadata["causation_id"].is_null(),
        "the reaper's emit has no causation: {metadata}",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");

    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_blob_policy_engine(
        &db,
        nats.nats().await,
        "se_s221",
        "pod-s221r",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(400)),
        policy,
        Duration::from_millis(150),
        true,
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let before = outbox_count(&pool).await;
    let reference = upload_verified(&pool, &executor, principal, tenant, &http).await;
    let object_key: String =
        sqlx::query_scalar("SELECT object_key FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&pool)
            .await
            .expect("read object key");
    wait_state(&pool, reference, "failed").await;

    let reason: String =
        sqlx::query_scalar("SELECT failed_reason FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&pool)
            .await
            .expect("read failed reason");
    assert!(
        reason.starts_with("policy:"),
        "the reason records the policy code: {reason}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while minio.object_exists(&bucket, &object_key).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refused object was not deleted"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        outbox_count(&pool).await,
        before,
        "a refused promotion commits no outbox row",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
