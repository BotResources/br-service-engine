mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_rls_query_service, passport_for};
use conformance_service_engine::sample::render::assignment;
use graphql_support::post_json;
use uuid::Uuid;

async fn rls_assignment(base_url: &str, passport: &str, id: Uuid) -> serde_json::Value {
    let query = format!("query {{ rlsAssignment(id: \"{id}\") {{ id title closed canClose }} }}");
    let (status, body) = post_json(base_url, Some(passport), &query, serde_json::json!({})).await;
    assert_eq!(status, 200, "the rls query answers over HTTP: {body}");
    body
}

#[tokio::test]
async fn s74_a_query_time_fetch_runs_under_the_registered_rls_applier_so_a_forbidden_key_is_absent()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    let mine = assignment(&pool, tenant, "alpha").await;

    let other_tenant = Uuid::now_v7();
    let theirs = assignment(&pool, other_tenant, "beta").await;

    let service = boot_rls_query_service(&db, nats.nats().await, "se_s74", "pod-s74").await;
    let passport = passport_for(user, tenant).to_header();

    let body = rls_assignment(&service.base_url, &passport, mine).await;
    assert_eq!(
        body["data"]["rlsAssignment"]["id"],
        serde_json::json!(mine.to_string()),
        "the principal's own row renders through the query path: {body}"
    );

    let body = rls_assignment(&service.base_url, &passport, theirs).await;
    assert!(
        body["data"]["rlsAssignment"].is_null(),
        "the projector's populate returns the forbidden key and its project applies no tenant \
         filter, so the row is hidden only because the query-time load ran under the registered \
         RLS applier's session context: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
