use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    boot_graphql_service, boot_undeclared_root_field,
};
use service_engine::EngineError;

#[tokio::test]
async fn s127_a_root_field_the_composed_schema_exposes_but_no_slice_declared_fails_the_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        boot_undeclared_root_field(&db, nats.nats().await, "se_s127", "pod-s127"),
    )
    .await
    .expect("the boot gate must reject an undeclared root field, not stand the pod up and serve");

    assert!(
        matches!(
            outcome,
            Err(EngineError::UndeclaredSchemaMember { ref member }) if member == "sampleAssignment"
        ),
        "the boot gate derives the root fields from the composed async-graphql schema and refuses \
         to serve when a real root field (`sampleAssignment`, exposed by the merged query root) is not \
         declared by any slice fragment, so an under-declared fragment cannot leave a real member \
         outside the collision gate: {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s127_two_subscription_slices_sharing_one_delta_union_boot_without_a_synthetic_slice() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s127b", "pod-s127b").await;

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
