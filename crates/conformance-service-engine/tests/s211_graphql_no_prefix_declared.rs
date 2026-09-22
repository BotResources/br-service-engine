use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::boot_without_prefix;
use service_engine::EngineError;

#[tokio::test]
async fn s211_fragments_registered_with_no_root_prefix_declared_fail_the_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        boot_without_prefix(&db, nats.nats().await, "se_s211", "pod-s211"),
    )
    .await
    .expect("the prefix gate must reject an undeclared prefix, not stand the pod up and serve");

    assert!(
        matches!(outcome, Err(EngineError::RootPrefixUndeclared)),
        "a real engine that registered schema slices but declared no root prefix refuses to serve \
         rather than exposing unprefixed roots that could shadow another service: {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}
