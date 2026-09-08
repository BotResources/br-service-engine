mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use conformance_service_engine::sample::render::member;
use graphql_support::{GraphqlWs, post_json};
use uuid::Uuid;

const SUBSCRIPTION: &str = "subscription { widgets { __typename \
    ... on ResetPayload { revision views { projector view } } \
    ... on UpsertPayload { revision view { projector view } } \
    ... on RemovePayload { revision projector } } }";

#[tokio::test]
async fn s60_a_subscription_resets_then_upserts_the_same_view_after_an_allowed_mutation() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "alpha").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s60", "pod-s60").await;
    let passport = passport_for(user, tenant).to_header();

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    ws.subscribe("1", SUBSCRIPTION).await;

    let reset = ws
        .next_data(Duration::from_secs(15))
        .await
        .expect("the attach delivers the opening Reset");
    let delta = &reset["widgets"];
    assert_eq!(delta["__typename"], serde_json::json!("ResetPayload"));
    assert_eq!(
        delta["revision"],
        serde_json::json!(1),
        "a session's first revision is 1: {reset}"
    );
    let view = &delta["views"][0]["view"];
    assert_eq!(
        view["closed"],
        serde_json::json!(false),
        "the opening view is the current state: {reset}"
    );
    assert_eq!(
        view["affordances"]["close"]["allowed"],
        serde_json::json!(true)
    );

    let mutation = format!("mutation {{ closeWidget(id: \"{widget}\") {{ success }} }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &mutation,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["data"]["closeWidget"]["success"],
        serde_json::json!(true)
    );

    let upsert = ws
        .next_data(Duration::from_secs(15))
        .await
        .expect("committing the close reaches the session as an Upsert");
    let delta = &upsert["widgets"];
    assert_eq!(delta["__typename"], serde_json::json!("UpsertPayload"));
    assert_eq!(
        delta["revision"],
        serde_json::json!(2),
        "the next delta advances the revision by one: {upsert}"
    );
    let view = &delta["view"]["view"];
    assert_eq!(
        view["closed"],
        serde_json::json!(true),
        "the committed state reaches the session, not the mutation response: {upsert}"
    );
    assert_eq!(
        view["affordances"]["close"]["allowed"],
        serde_json::json!(false)
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
