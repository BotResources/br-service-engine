use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine_config;
use service_engine::error::EngineError;
use service_engine::{Engine, ReadinessHandle};

#[tokio::test]
async fn s274_a_zero_blob_reaper_interval_is_refused_at_boot_and_claims_nothing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let refused = Engine::<SamplePrincipal>::boot(
        engine_config("se_s274", "pod-s274").with_blob_reaper_interval(Duration::ZERO),
        pool.clone(),
        nats.nats().await,
        ReadinessHandle::not_ready("boot"),
    )
    .await
    .expect_err("a zero blob reaper interval must fail boot, not leave uploads pending for ever");
    assert!(
        matches!(&refused, EngineError::Config(message) if message.contains("blob_reaper_interval")),
        "the refusal is a configuration error naming the field: {refused:?}",
    );

    let claims: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.schema_version")
        .fetch_one(&pool)
        .await
        .expect("count the schema version claims");
    assert_eq!(
        claims, 0,
        "a refused configuration is refused before the pod claims the store"
    );

    Engine::<SamplePrincipal>::boot(
        engine_config("se_s274", "pod-s274"),
        pool,
        nats.nats().await,
        ReadinessHandle::not_ready("boot"),
    )
    .await
    .expect("the same pod boots with the default reaper interval");

    drop(nats);
    db.cleanup().await;
}
