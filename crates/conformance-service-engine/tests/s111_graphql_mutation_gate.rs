mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use graphql_support::post_json;
use uuid::Uuid;

#[tokio::test]
async fn s111_a_graphql_mutation_denies_with_the_affordance_reason_code_and_allows_over_one_pipeline()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "alpha").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s111", "pod-s111").await;
    let passport = passport_for(user, tenant).to_header();

    let query = format!("mutation {{ closeWidget(id: \"{widget}\") {{ success }} }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &query,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "an allowed mutation answers over HTTP: {body}");
    assert_eq!(
        body["data"]["closeWidget"]["success"],
        serde_json::json!(true),
        "the pipeline commits and the synchronous channel answers success: {body}"
    );

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &query,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 200,
        "a denied mutation is still a well-formed graphql response: {body}"
    );
    assert_eq!(
        body["errors"][0]["extensions"]["code"],
        serde_json::json!("ALREADY_CLOSED"),
        "the same gate that blocks the affordance refuses the mutation with its own reason code: {body}"
    );
    assert!(
        body["data"]["closeWidget"].is_null(),
        "a refused mutation carries no success payload: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
