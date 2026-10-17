mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    FactResolver, boot_bare_schema_fact_service, boot_fact_service_resolving, passport_for,
};
use conformance_service_engine::sample::render::{member, move_member};
use graphql_support::GraphqlWs;
use uuid::Uuid;

const WITHIN: Duration = Duration::from_secs(15);
const FACT_QUERY: &str = "query { sampleRegisteredTenant }";

async fn answer(ws: &mut GraphqlWs, id: &str) -> serde_json::Value {
    ws.subscribe(id, FACT_QUERY).await;
    let payload = ws
        .next_payload(WITHIN)
        .await
        .expect("the operation is answered on a next frame");
    ws.expect_complete(WITHIN).await;
    payload
}

async fn set_fact_grant(db: &TestDb, grant: bool) {
    let statement = if grant {
        format!("GRANT SELECT ON sample_member TO \"{}\"", db.app_role())
    } else {
        format!("REVOKE SELECT ON sample_member FROM \"{}\"", db.app_role())
    };
    sqlx::query(&statement)
        .execute(db.owner_pool())
        .await
        .expect("the owner changes the app role's grant on the fact table");
}

#[tokio::test]
async fn s273_a_socket_operation_reads_the_facts_of_its_own_moment_not_those_of_the_upgrade() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let old_tenant = Uuid::now_v7();
    let new_tenant = Uuid::now_v7();
    member(&pool, user, old_tenant).await;

    let service = boot_fact_service_resolving(
        &db,
        nats.nats().await,
        "se_s273a",
        "pod-s273a",
        FactResolver::Echoing,
    )
    .await;
    let passport = passport_for(user, old_tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    let before = answer(&mut ws, "1").await;
    assert_eq!(
        before["data"]["sampleRegisteredTenant"],
        serde_json::json!(old_tenant),
        "control: the fact the upgrade loaded answers the first operation: {before}"
    );

    move_member(&pool, user, new_tenant).await;

    let after = answer(&mut ws, "2").await;
    assert_eq!(
        after["data"]["sampleRegisteredTenant"],
        serde_json::json!(new_tenant),
        "the resolver only echoes the principal, so only the fact loader can have moved the \
         tenant: a second operation on the same socket sees the fact of this moment: {after}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s273_a_fact_loader_fault_on_a_socket_operation_is_internal_and_leaves_the_socket_usable() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    member(&pool, user, tenant).await;

    let service = boot_fact_service_resolving(
        &db,
        nats.nats().await,
        "se_s273b",
        "pod-s273b",
        FactResolver::Echoing,
    )
    .await;
    let passport = passport_for(user, tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    let healthy = answer(&mut ws, "1").await;
    assert_eq!(
        healthy["data"]["sampleRegisteredTenant"],
        serde_json::json!(tenant),
        "control: the operation is served while the fact table is readable: {healthy}"
    );

    set_fact_grant(&db, false).await;

    let faulted = answer(&mut ws, "2").await;
    assert!(
        faulted["data"].is_null(),
        "a fact the pod cannot load carries no datum: {faulted}"
    );
    assert_eq!(
        (
            &faulted["errors"][0]["extensions"]["code"],
            &faulted["errors"][0]["message"]
        ),
        (
            &serde_json::json!("INTERNAL"),
            &serde_json::json!("internal error")
        ),
        "the resolver succeeded, so the fault is the loader's: INTERNAL, never UNAUTHENTICATED: \
         {faulted}"
    );
    let text = faulted.to_string();
    for leak in ["permission", "denied", "sample_member", db.app_role()] {
        assert!(
            !text.contains(leak),
            "the client learns the code, never the database text (`{leak}`): {text}"
        );
    }

    set_fact_grant(&db, true).await;

    let restored = answer(&mut ws, "3").await;
    assert_eq!(
        restored["data"]["sampleRegisteredTenant"],
        serde_json::json!(tenant),
        "the fault is per operation: the same socket is served once the grant is back: {restored}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s273_a_socket_operation_without_a_registered_principal_resolver_is_internal_while_post_is_served()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    member(&pool, user, tenant).await;

    let service = boot_fact_service_resolving(
        &db,
        nats.nats().await,
        "se_s273c",
        "pod-s273c",
        FactResolver::Absent,
    )
    .await;
    let passport = passport_for(user, tenant).to_header();

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    let refused = answer(&mut ws, "1").await;
    assert!(
        refused["data"].is_null(),
        "an operation that cannot resolve its principal carries no datum: {refused}"
    );
    assert_eq!(
        refused["errors"][0]["extensions"]["code"],
        serde_json::json!("INTERNAL"),
        "a missing resolver is a service fault, never UNAUTHENTICATED: {refused}"
    );

    let response = reqwest::Client::new()
        .post(service.http("/graphql"))
        .header("x-passport", &passport)
        .json(&serde_json::json!({ "query": FACT_QUERY }))
        .send()
        .await
        .expect("the POST reaches the server");
    assert_eq!(response.status().as_u16(), 200, "POST needs no resolver");
    let body: serde_json::Value = response.json().await.expect("a GraphQL response");
    assert_eq!(
        body["data"]["sampleRegisteredTenant"],
        serde_json::json!(tenant),
        "POST builds its principal at the request and is untouched: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s273_a_schema_not_built_by_engine_schema_never_serves_the_upgrade_time_principal_on_a_socket()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    member(&pool, user, tenant).await;

    let service =
        boot_bare_schema_fact_service(&db, nats.nats().await, "se_s273d", "pod-s273d").await;
    let passport = passport_for(user, tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    let refused = answer(&mut ws, "1").await;
    assert!(
        refused["data"].is_null(),
        "without the engine extension the socket holds no principal a resolver can read: {refused}"
    );
    assert!(
        !refused.to_string().contains(&tenant.to_string()),
        "the upgrade-time fact is not served: {refused}"
    );

    let response = reqwest::Client::new()
        .post(service.http("/graphql"))
        .header("x-passport", &passport)
        .json(&serde_json::json!({ "query": FACT_QUERY }))
        .send()
        .await
        .expect("the POST reaches the server");
    let body: serde_json::Value = response.json().await.expect("a GraphQL response");
    assert_eq!(
        body["data"]["sampleRegisteredTenant"],
        serde_json::json!(tenant),
        "POST carries its own fresh principal and needs no extension: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
