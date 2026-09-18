mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::render::member;
use graphql_support::{GraphqlWs, post_json};
use uuid::Uuid;

const SUBSCRIPTION: &str = "subscription { widgets { __typename \
    ... on ResetPayload { revision views { __typename ... on WidgetView { id label } } } \
    ... on UpsertPayload { revision view { __typename ... on WidgetView { id label } } } } }";

#[tokio::test]
async fn s113_a_one_shot_secret_rides_only_the_mutation_response_and_never_the_subscription() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, tenant).await;
    let widget = Uuid::now_v7();

    let service = boot_graphql_service(&db, nats.nats().await, "se_s113", "pod-s113").await;
    let passport = passport_for(user, tenant).to_header();

    let mutation = format!(
        "mutation {{ mintSecret(id: \"{widget}\", label: \"vault\", tenant: \"{tenant}\") }}"
    );
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &mutation,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200);
    let secret = body["data"]["mintSecret"]
        .as_str()
        .expect("the one-shot secret is returned on the synchronous channel")
        .to_string();
    assert_eq!(secret, format!("secret-for-{widget}"));

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    ws.subscribe("1", SUBSCRIPTION).await;
    let reset = ws
        .next_data(Duration::from_secs(15))
        .await
        .expect("the attach delivers the committed widget in a Reset");
    assert!(
        !reset.to_string().contains(&secret),
        "no subscription frame carries the one-shot secret: {reset}"
    );
    let view = reset["widgets"]["views"]
        .as_array()
        .and_then(|views| {
            views
                .iter()
                .find(|entry| entry["id"] == serde_json::json!(widget.to_string()))
        })
        .expect("the minted widget is in the view");
    assert_eq!(view["label"], serde_json::json!("vault"));

    let outbox: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.integration_outbox")
        .fetch_one(&pool)
        .await
        .expect("count outbox rows");
    assert_eq!(outbox, 0, "a one-shot secret never becomes an outbox row");

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
