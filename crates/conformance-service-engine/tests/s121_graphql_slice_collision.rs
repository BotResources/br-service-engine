use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    boot_colliding_slices, root_field_collision, type_collision,
};
use service_engine::EngineError;

#[tokio::test]
async fn s121_two_slices_claiming_the_same_root_field_fail_the_engine_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let outcome = boot_colliding_slices(
        &db,
        nats.nats().await,
        "se_s66rf",
        "pod-s66rf",
        root_field_collision(),
    )
    .await;

    assert!(
        matches!(
            outcome,
            Err(EngineError::DuplicateSchemaMember {
                kind: "root field",
                first: "widget",
                second: "shadow",
                ref member,
            }) if member == "sampleWidget"
        ),
        "a real engine that registered two slices claiming the same root field refuses to serve, \
         naming both slices, instead of standing up an ambiguous schema: {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s121_two_slices_claiming_the_same_type_fail_the_engine_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let outcome = boot_colliding_slices(
        &db,
        nats.nats().await,
        "se_s66ty",
        "pod-s66ty",
        type_collision(),
    )
    .await;

    assert!(
        matches!(
            outcome,
            Err(EngineError::DuplicateSchemaMember {
                kind: "type",
                first: "widget",
                second: "shadow",
                ref member,
            }) if member == "WidgetView"
        ),
        "the same boot gate rejects two slices that claim the same GraphQL type, not only a root \
         field: {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}
