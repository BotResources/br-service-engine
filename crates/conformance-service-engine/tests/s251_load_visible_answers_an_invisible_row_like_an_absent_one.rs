mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_reads_service, passport_for};
use conformance_service_engine::sample::render::assignment;
use graphql_support::post_json;
use uuid::Uuid;

async fn gated_load(base_url: &str, passport: &str, field: &str, id: Uuid) -> serde_json::Value {
    let query = format!("query {{ {field}(id: \"{id}\") {{ id title }} }}");
    let (status, body) = post_json(base_url, Some(passport), &query, serde_json::json!({})).await;
    assert_eq!(status, 200, "the gated load answers over HTTP: {body}");
    body
}

#[tokio::test]
async fn s251_a_row_outside_the_principal_cohorts_answers_exactly_like_an_absent_key() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let mine = assignment(&pool, tenant, "alpha").await;
    let theirs = assignment(&pool, Uuid::now_v7(), "beta").await;
    let absent = Uuid::now_v7();

    let service = boot_reads_service(&db, nats.nats().await, "pod-s251", true).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();
    let field = "sampleVisibleAssignment";

    let body = gated_load(&service.base_url, &passport, field, mine).await;
    assert_eq!(
        body["data"][field]["id"],
        serde_json::json!(mine.to_string()),
        "a row inside the principal's cohorts loads: {body}"
    );

    let hidden = gated_load(&service.base_url, &passport, field, theirs).await;
    let missing = gated_load(&service.base_url, &passport, field, absent).await;
    assert!(
        hidden["data"][field].is_null(),
        "a row outside the principal's cohorts is not returned: {hidden}"
    );
    assert_eq!(
        hidden, missing,
        "a hidden row and an absent key produce the same answer, so the caller cannot probe \
         whether a key exists"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s251_a_view_declared_under_rls_loads_under_the_registered_applier() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let mine = assignment(&pool, tenant, "alpha").await;
    let theirs = assignment(&pool, Uuid::now_v7(), "beta").await;

    let service = boot_reads_service(&db, nats.nats().await, "pod-s251-rls", true).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();
    let field = "sampleRlsVisibleAssignment";

    let body = gated_load(&service.base_url, &passport, field, mine).await;
    assert_eq!(
        body["data"][field]["id"],
        serde_json::json!(mine.to_string()),
        "the principal's own row loads under RLS: {body}"
    );

    let body = gated_load(&service.base_url, &passport, field, theirs).await;
    assert!(
        body["data"][field].is_null() && body.get("errors").is_none(),
        "the view's visibility is Unrestricted, so the other tenant's row is hidden only \
         because the load ran under the principal's RLS context: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
