mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use conformance_service_engine::sample::render::{forget_member, member};
use graphql_support::GraphqlWs;
use uuid::Uuid;

const WITHIN: Duration = Duration::from_secs(15);
const SUBSCRIPTION: &str = "subscription { sampleWidgets { __typename \
    ... on ResetPayload { revision views { __typename ... on WidgetView { id } } } } }";

async fn expect_unauthenticated(ws: &mut GraphqlWs, label: &str) {
    let payload = ws
        .next_payload(WITHIN)
        .await
        .unwrap_or_else(|| panic!("{label}: the refusal is framed as a next payload"));
    assert!(
        payload["data"].is_null(),
        "{label}: a refused operation carries no datum: {payload}"
    );
    assert_eq!(
        payload["errors"][0]["extensions"]["code"],
        serde_json::json!("UNAUTHENTICATED"),
        "{label}: a principal that resolves to nothing is refused UNAUTHENTICATED: {payload}"
    );
    ws.expect_complete(WITHIN).await;
}

#[tokio::test]
async fn s271_a_new_operation_on_a_socket_after_a_revocation_is_refused_not_served_with_stale_facts()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let revoked = Uuid::now_v7();
    let bystander = Uuid::now_v7();
    member(&pool, revoked, tenant).await;
    member(&pool, bystander, tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "alpha").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s271", "pod-s271").await;
    let passport = passport_for(revoked, tenant).to_header();
    let bystander_passport = passport_for(bystander, tenant).to_header();

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    let mut other = GraphqlWs::connect(&service.base_url, &bystander_passport).await;

    ws.subscribe("1", SUBSCRIPTION).await;
    let first = ws
        .next_data(WITHIN)
        .await
        .expect("the first attach on a live principal delivers its Reset");
    assert_eq!(first["sampleWidgets"]["__typename"], "ResetPayload");

    ws.subscribe("2", SUBSCRIPTION).await;
    let second = ws
        .next_data(WITHIN)
        .await
        .expect("a second attach on the same socket is served while the principal exists");
    assert_eq!(
        second["sampleWidgets"]["views"][0]["id"],
        serde_json::json!(widget),
        "control: the second attach sees the widget the principal may read: {second}"
    );

    forget_member(&pool, revoked).await;

    ws.subscribe("3", SUBSCRIPTION).await;
    expect_unauthenticated(&mut ws, "a subscription attached after the revocation").await;

    ws.subscribe(
        "4",
        &format!("mutation {{ sampleCloseWidget(id: \"{widget}\") {{ success }} }}"),
    )
    .await;
    expect_unauthenticated(&mut ws, "a mutation sent after the revocation").await;

    ws.subscribe("5", &format!("query {{ sampleWidget(id: \"{widget}\") }}"))
        .await;
    expect_unauthenticated(&mut ws, "a query sent after the revocation").await;

    other.subscribe("1", SUBSCRIPTION).await;
    let served = other
        .next_data(WITHIN)
        .await
        .expect("the revocation of one principal leaves another principal's socket served");
    assert_eq!(served["sampleWidgets"]["__typename"], "ResetPayload");

    member(&pool, revoked, tenant).await;
    ws.subscribe("6", SUBSCRIPTION).await;
    let restored = ws
        .next_data(WITHIN)
        .await
        .expect("the same socket is served again once the principal exists again");
    assert_eq!(
        restored["sampleWidgets"]["__typename"], "ResetPayload",
        "the refusal is per operation: the socket stays well-defined and is not poisoned: {restored}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s271_a_new_subscription_on_a_socket_loads_the_facts_of_the_moment_not_those_of_the_upgrade()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let old_tenant = Uuid::now_v7();
    let new_tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, old_tenant).await;
    let old_widget = Uuid::now_v7();
    let new_widget = Uuid::now_v7();
    insert_widget(&pool, old_widget, old_tenant, "old").await;
    insert_widget(&pool, new_widget, new_tenant, "new").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s271f", "pod-s271f").await;
    let passport = passport_for(user, old_tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    ws.subscribe("1", SUBSCRIPTION).await;
    let before = ws.next_data(WITHIN).await.expect("the first Reset");
    assert_eq!(
        before["sampleWidgets"]["views"][0]["id"],
        serde_json::json!(old_widget),
        "control: the upgrade-time tenant decides the first window: {before}"
    );

    conformance_service_engine::sample::render::move_member(&pool, user, new_tenant).await;

    ws.subscribe("2", SUBSCRIPTION).await;
    let after = ws.next_data(WITHIN).await.expect("the second Reset");
    assert_eq!(
        after["sampleWidgets"]["views"][0]["id"],
        serde_json::json!(new_widget),
        "the attach resolved the principal again, so it sees the tenant of this moment, never \
         the tenant the socket was upgraded under: {after}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
