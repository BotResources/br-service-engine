mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_reads_service, passport_for};
use conformance_service_engine::sample::latch::AdvisoryLatch;
use conformance_service_engine::sample::render::assignment;
use graphql_support::post_json;
use uuid::Uuid;

const JOURNAL: &str = "query { sampleAssignmentJournal }";

fn ids(body: &serde_json::Value) -> Vec<String> {
    body["data"]["sampleAssignmentJournal"]
        .as_array()
        .unwrap_or_else(|| panic!("the journal answers a list: {body}"))
        .iter()
        .map(|id| id.as_str().expect("an id is a string").to_string())
        .collect()
}

fn code(body: &serde_json::Value) -> Option<&str> {
    body["errors"][0]["extensions"]["code"].as_str()
}

#[tokio::test]
async fn s252_hand_sql_with_no_tenant_filter_returns_only_the_rows_inside_the_principal_rls_scope()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let mut mine = vec![
        assignment(&pool, tenant, "alpha").await.to_string(),
        assignment(&pool, tenant, "gamma").await.to_string(),
    ];
    mine.sort();
    let theirs = assignment(&pool, Uuid::now_v7(), "beta").await;

    let service = boot_reads_service(&db, nats.nats().await, "pod-s252", true).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        JOURNAL,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "the journal answers over HTTP: {body}");
    assert_eq!(
        ids(&body),
        mine,
        "the hand SQL selects every row, and the RLS context the engine applied keeps only the \
         principal's tenant: {body}"
    );
    assert!(!ids(&body).contains(&theirs.to_string()));

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s252_a_write_inside_the_read_is_refused_and_leaves_nothing_behind() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let service = boot_reads_service(&db, nats.nats().await, "pod-s252-write", true).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();
    let smuggled = Uuid::now_v7();

    let query =
        format!("query {{ sampleJournalWriteAttempt(id: \"{smuggled}\", tenant: \"{tenant}\") }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &query,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "the attempt answers over HTTP: {body}");
    assert_eq!(
        code(&body),
        Some("INTERNAL"),
        "the read transaction is read-only, so the INSERT fails: {body}"
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM sample_assignment WHERE id = $1")
        .bind(smuggled)
        .fetch_one(&pool)
        .await
        .expect("count the smuggled row");
    assert_eq!(written, 0, "nothing the read attempted to write persists");

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s252_without_a_registered_applier_the_read_fails_closed() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    assignment(&pool, tenant, "alpha").await;
    assignment(&pool, Uuid::now_v7(), "beta").await;

    let service = boot_reads_service(&db, nats.nats().await, "pod-s252-bare", false).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        JOURNAL,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "the journal answers over HTTP: {body}");
    assert_eq!(
        code(&body),
        Some("INTERNAL"),
        "with no RlsApplier the engine refuses to run the hand SQL rather than run it with no \
         RLS context: {body}"
    );
    assert!(
        body["data"].is_null() || body["data"]["sampleAssignmentJournal"].is_null(),
        "no row leaks out of a refused read: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s252_every_statement_of_the_read_sees_one_snapshot() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    assignment(&pool, tenant, "alpha").await;
    assignment(&pool, tenant, "gamma").await;
    let service = boot_reads_service(&db, nats.nats().await, "pod-s252-snapshot", true).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let mut latch = AdvisoryLatch::hold(db.owner_pool()).await;
    let pending = tokio::spawn({
        let base_url = service.base_url.clone();
        let passport = passport.clone();
        let query = format!(
            "query {{ sampleJournalAcrossLatch(latch: {}) }}",
            latch.key()
        );
        async move { post_json(&base_url, Some(&passport), &query, serde_json::json!({})).await }
    });
    latch.await_waiter(Duration::from_secs(10)).await;
    assignment(
        db.owner_pool(),
        tenant,
        "committed between the two statements",
    )
    .await;
    latch.release().await;
    let (status, body) = pending.await.expect("the read answers");
    assert_eq!(status, 200, "the read answers over HTTP: {body}");
    assert_eq!(
        body["data"]["sampleJournalAcrossLatch"],
        serde_json::json!([2, 2]),
        "a row committed between two statements of the read is seen by neither: the read is one \
         snapshot: {body}"
    );

    let (_, later) = post_json(
        &service.base_url,
        Some(&passport),
        JOURNAL,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        ids(&later).len(),
        3,
        "the row was committed; the next read sees it: {later}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
