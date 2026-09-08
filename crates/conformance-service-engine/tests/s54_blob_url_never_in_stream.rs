#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::DetachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, reset_views, upsert_cause, upserted, window,
};
use conformance_service_engine::sample::{DocProjector, DocView};
use engine_twin::{SOON, await_ready};
use service_engine::BlobPolicy;
use uuid::Uuid;

#[tokio::test]
async fn s54_a_presigned_url_never_enters_a_view_or_an_impact_cause() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s54-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let doc = Uuid::now_v7();
    let reference = Uuid::now_v7();
    sqlx::query("INSERT INTO sample_doc (id, tenant_id, name, blob_ref) VALUES ($1, $2, $3, $4)")
        .bind(doc)
        .bind(tenant)
        .bind("secret.png")
        .bind(reference)
        .execute(&pool)
        .await
        .expect("seed a doc that already references a blob");

    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s54",
        "pod-s54",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(DocProjector::NAME, false)],
        ))
        .await
        .expect("the doc session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let reset_json = serde_json::to_string(&reset).expect("serialize the Reset");
    assert!(
        !reset_json.contains("X-Amz") && !reset_json.contains("127.0.0.1"),
        "the Reset carries the reference, never a presigned URL: {reset_json}",
    );
    let seeded: DocView = reset_views(&reset)[0]
        .view
        .decode()
        .expect("the seeded view decodes");
    assert_eq!(
        seeded.blob_ref,
        Some(reference),
        "the view carries the opaque reference"
    );

    executor
        .run::<DetachDoc>(principal, DetachDoc { id: doc })
        .await
        .expect("detaching commits and impacts the view");

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the detach reaches the session as an Upsert");
    let json = serde_json::to_string(&delta).expect("serialize the delta");
    assert!(
        !json.contains("X-Amz") && !json.contains("Signature") && !json.contains("127.0.0.1"),
        "no presigned URL material rides the delivery stream: {json}",
    );
    let view: DocView = upserted(&delta).view.decode().expect("the view decodes");
    assert_eq!(view.blob_ref, None, "the released view drops its reference");
    let cause = upsert_cause(&delta).expect("the delta carries its cause");
    assert!(cause.contains("detached") && !cause.contains("X-Amz"));

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
