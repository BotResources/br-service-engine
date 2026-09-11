mod graphql_support;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::boot_graphql_service;
use graphql_support::post_json;
use uuid::Uuid;

#[tokio::test]
async fn s115_a_request_without_a_trusted_passport_is_rejected_before_the_pipeline() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s115", "pod-s115").await;

    let widget = Uuid::now_v7();
    let mutation = format!("mutation {{ closeWidget(id: \"{widget}\") {{ success }} }}");
    let (status, _body) =
        post_json(&service.base_url, None, &mutation, serde_json::json!({})).await;
    assert_eq!(
        status, 401,
        "the kit does authZ only: a request with no X-Passport never reaches a resolver"
    );

    let (status, _body) = post_json(
        &service.base_url,
        Some("not-a-valid-passport"),
        &mutation,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 401,
        "a malformed X-Passport fails closed rather than defaulting to a principal"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
