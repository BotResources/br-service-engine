mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use conformance_service_engine::sample::render::{assignment, member};
use graphql_support::GraphqlWs;
use uuid::Uuid;

const WIDGETS: &str = "subscription { widgets { __typename \
    ... on ResetPayload { views { __typename } } } }";
const ASSIGNMENTS: &str = "subscription { assignments { __typename \
    ... on ResetPayload { views { __typename } } } }";

fn reset_typenames(reset: &serde_json::Value, field: &str) -> Vec<String> {
    reset[field]["views"]
        .as_array()
        .expect("a Reset carries its views")
        .iter()
        .map(|view| view["__typename"].as_str().unwrap_or_default().to_string())
        .collect()
}

#[tokio::test]
async fn s65_two_projectors_are_two_typed_union_members_and_a_client_sees_only_its_own() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "alpha").await;
    assignment(&pool, tenant, "review").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s65", "pod-s65").await;
    let passport = passport_for(user, tenant).to_header();

    let mut widgets = GraphqlWs::connect(&service.base_url, &passport).await;
    widgets.subscribe("1", WIDGETS).await;
    let reset = widgets
        .next_data(Duration::from_secs(15))
        .await
        .expect("the widget subscription resets");
    let members = reset_typenames(&reset, "widgets");
    assert!(
        !members.is_empty() && members.iter().all(|m| m == "WidgetView"),
        "a client subscribing to widgets receives only the WidgetView union member: {reset}"
    );

    let mut assignments = GraphqlWs::connect(&service.base_url, &passport).await;
    assignments.subscribe("1", ASSIGNMENTS).await;
    let reset = assignments
        .next_data(Duration::from_secs(15))
        .await
        .expect("the assignment subscription resets");
    let members = reset_typenames(&reset, "assignments");
    assert!(
        !members.is_empty() && members.iter().all(|m| m == "AssignmentView"),
        "the second projector delivers only its own AssignmentView member, never the widget's: {reset}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
