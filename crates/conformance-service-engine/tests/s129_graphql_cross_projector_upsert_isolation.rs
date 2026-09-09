mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use conformance_service_engine::sample::render::{assignment, member};
use graphql_support::{GraphqlWs, post_json};
use uuid::Uuid;

const WIDGETS: &str = "subscription { widgets { __typename \
    ... on ResetPayload { revision } \
    ... on UpsertPayload { revision view { __typename ... on WidgetView { closed } } } } }";
const ASSIGNMENTS: &str = "subscription { assignments { __typename \
    ... on ResetPayload { revision } \
    ... on UpsertPayload { view { __typename } } } }";

#[tokio::test]
async fn s129_a_live_upsert_reaches_only_its_own_projector_subscription() {
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

    let service = boot_graphql_service(&db, nats.nats().await, "se_s129", "pod-s129").await;
    let passport = passport_for(user, tenant).to_header();

    let mut widgets = GraphqlWs::connect(&service.base_url, &passport).await;
    widgets.subscribe("1", WIDGETS).await;
    let reset = widgets
        .next_data(Duration::from_secs(15))
        .await
        .expect("the widget subscription opens with a Reset");
    assert_eq!(
        reset["widgets"]["__typename"],
        serde_json::json!("ResetPayload")
    );

    let mut assignments = GraphqlWs::connect(&service.base_url, &passport).await;
    assignments.subscribe("1", ASSIGNMENTS).await;
    let reset = assignments
        .next_data(Duration::from_secs(15))
        .await
        .expect("the assignment subscription opens with a Reset");
    assert_eq!(
        reset["assignments"]["__typename"],
        serde_json::json!("ResetPayload")
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

    let upsert = widgets
        .next_data(Duration::from_secs(15))
        .await
        .expect("closing the widget reaches the widget subscription as an Upsert");
    let delta = &upsert["widgets"];
    assert_eq!(delta["__typename"], serde_json::json!("UpsertPayload"));
    assert_eq!(
        delta["revision"],
        serde_json::json!(2),
        "the widget session's live delta advances its own revision by one: {upsert}"
    );
    assert_eq!(
        delta["view"]["__typename"],
        serde_json::json!("WidgetView"),
        "a live Upsert carries the mutating projector's own union member: {upsert}"
    );
    assert_eq!(delta["view"]["closed"], serde_json::json!(true));

    assert!(
        assignments
            .next_data(Duration::from_secs(2))
            .await
            .is_none(),
        "a widget mutation stages an impact on the widget projector only, so the \
         assignment subscription — a different member of the typed union — receives no live delta"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
