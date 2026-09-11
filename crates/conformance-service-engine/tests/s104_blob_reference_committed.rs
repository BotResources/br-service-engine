#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::BlobPolicy;
use sqlx::PgPool;
use uuid::Uuid;

async fn doc_count(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_doc WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count docs")
}

async fn blob_count(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count blob references")
}

async fn blob_ref_of(pool: &PgPool, doc: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_optional(pool)
        .await
        .expect("read the doc's blob reference")
        .flatten()
}

#[tokio::test]
async fn s104_a_blob_reference_row_commits_with_the_aggregate_and_a_rollback_leaves_neither() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s104-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s104",
        "pod-s104",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let rolled_back = Uuid::now_v7();
    let refused = executor
        .run::<AttachDoc>(
            principal.clone(),
            AttachDoc {
                id: rolled_back,
                tenant,
                name: "reject.png".to_string(),
                content_type: "image/png".to_string(),
                fail: true,
            },
        )
        .await;
    assert!(
        refused.is_err(),
        "the handler failed after staging the blob"
    );
    assert_eq!(doc_count(&pool, rolled_back).await, 0, "no aggregate row");
    assert_eq!(
        blob_count(&pool, rolled_back).await,
        0,
        "the staged blob reference rolled back with the aggregate",
    );

    let committed = Uuid::now_v7();
    executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: committed,
                tenant,
                name: "keep.png".to_string(),
                content_type: "image/png".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits");
    assert_eq!(doc_count(&pool, committed).await, 1);
    let reference = blob_ref_of(&pool, committed)
        .await
        .expect("the committed doc references a blob");
    assert_eq!(
        blob_count(&pool, reference).await,
        1,
        "the blob reference committed in the same transaction as the aggregate",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
