mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_reads_service, passport_for};
use conformance_service_engine::sample::render::{assignment, note};
use graphql_support::post_json;
use uuid::Uuid;

async fn ask(base_url: &str, passport: &str, query: &str) -> serde_json::Value {
    let (status, body) = post_json(base_url, Some(passport), query, serde_json::json!({})).await;
    assert_eq!(status, 200, "the read answers over HTTP: {body}");
    body
}

fn notes_of(id: Uuid) -> String {
    format!("query {{ sampleAssignmentNotes(id: \"{id}\") }}")
}

#[tokio::test]
async fn s254_the_children_of_a_visible_parent_are_read_and_a_hidden_parent_answers_like_an_absent_one()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let mine = assignment(&pool, tenant, "alpha").await;
    note(&pool, mine, 2, "second").await;
    note(&pool, mine, 1, "first").await;
    let theirs = assignment(&pool, Uuid::now_v7(), "beta").await;
    note(&pool, theirs, 1, "not yours").await;
    let absent = Uuid::now_v7();

    let service = boot_reads_service(&db, nats.nats().await, "pod-s254", false).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let body = ask(&service.base_url, &passport, &notes_of(mine)).await;
    assert_eq!(
        body["data"]["sampleAssignmentNotes"],
        serde_json::json!(["first", "second"]),
        "the parent is inside the principal's cohorts, so the hand SQL behind it runs, with no \
         RlsApplier registered: {body}"
    );

    let hidden = ask(&service.base_url, &passport, &notes_of(theirs)).await;
    let missing = ask(&service.base_url, &passport, &notes_of(absent)).await;
    assert!(
        hidden["data"]["sampleAssignmentNotes"].is_null() && hidden.get("errors").is_none(),
        "the children of a parent outside the principal's cohorts are never read: {hidden}"
    );
    assert_eq!(
        hidden, missing,
        "a hidden parent and an absent key produce the same answer, so the caller cannot probe \
         whether a key exists"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s254_a_write_behind_a_visible_parent_is_refused_and_leaves_nothing_behind() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let mine = assignment(&pool, tenant, "alpha").await;
    let service = boot_reads_service(&db, nats.nats().await, "pod-s254-write", false).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let body = ask(
        &service.base_url,
        &passport,
        &format!("query {{ sampleNoteWriteAttempt(id: \"{mine}\") }}"),
    )
    .await;
    assert_eq!(
        body["errors"][0]["extensions"]["code"].as_str(),
        Some("INTERNAL"),
        "the read behind the parent runs in a read-only transaction, so the INSERT fails: {body}"
    );
    let written: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_note WHERE assignment_id = $1")
            .bind(mine)
            .fetch_one(&pool)
            .await
            .expect("count the smuggled note");
    assert_eq!(written, 0, "nothing the read attempted to write persists");

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
