use service_engine::error::CompositionError;
use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::boot_field_outside_prefix;
use service_engine::EngineError;

#[tokio::test]
async fn s210_a_root_field_outside_the_declared_prefix_fails_the_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        boot_field_outside_prefix(&db, nats.nats().await, "se_s210", "pod-s210"),
    )
    .await
    .expect("the prefix gate must reject the fragment, not stand the pod up and serve");

    assert!(
        matches!(
            outcome,
            Err(EngineError::Composition(CompositionError::RootFieldOutsidePrefix {
                slice: "outsider",
                ref field,
                ref prefix,
            })) if field == "outsider" && prefix == "sample"
        ),
        "a real engine that declared the `sample` root prefix refuses to serve when a registered \
         fragment exposes a root field outside it, naming the slice, the field and the prefix: \
         {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}
