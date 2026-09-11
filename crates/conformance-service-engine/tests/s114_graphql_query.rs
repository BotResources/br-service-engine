mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use graphql_support::post_json;
use uuid::Uuid;

#[tokio::test]
async fn s114_a_query_returns_the_rendered_view_with_affordances_and_hides_what_the_principal_cannot_see()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    let mine = Uuid::now_v7();
    insert_widget(&pool, mine, tenant, "alpha").await;

    let other_tenant = Uuid::now_v7();
    let theirs = Uuid::now_v7();
    insert_widget(&pool, theirs, other_tenant, "beta").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s114", "pod-s114").await;
    let passport = passport_for(user, tenant).to_header();

    let query = format!("query {{ widget(id: \"{mine}\") }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &query,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "the query answers over HTTP: {body}");
    let view = &body["data"]["widget"];
    assert_eq!(view["id"], serde_json::json!(mine.to_string()));
    assert_eq!(view["closed"], serde_json::json!(false));
    assert_eq!(
        view["affordances"]["close"]["allowed"],
        serde_json::json!(true),
        "the query view carries the same affordances as a subscription frame: {body}"
    );

    let hidden = format!("query {{ widget(id: \"{theirs}\") }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &hidden,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["data"]["widget"].is_null(),
        "a key the principal may not see answers as absent, never another tenant's row: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
