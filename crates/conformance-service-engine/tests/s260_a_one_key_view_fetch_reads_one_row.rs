mod graphql_support;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::counted::{ceilings_asked_for, rows_read_of};
use conformance_service_engine::sample::graphql::{boot_counted_service, passport_for};
use conformance_service_engine::sample::render::*;
use graphql_support::post_json;
use service_engine::WINDOW_TOO_LARGE_CODE;
use uuid::Uuid;

const CAPACITY: usize = 4;

async fn item(base_url: &str, passport: &str, id: Uuid) -> serde_json::Value {
    let (status, body) = post_json(
        base_url,
        Some(passport),
        "query($id: UUID!) { sampleCountedItem(id: $id) { id } }",
        serde_json::json!({ "id": id }),
    )
    .await;
    assert_eq!(status, 200, "the query answers over HTTP: {body}");
    assert!(
        body["errors"].is_null(),
        "a one-key fetch is not refused: {body}"
    );
    body
}

#[tokio::test]
async fn s260_a_one_key_view_fetch_reads_its_row_and_never_the_population() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let mut collection = Vec::with_capacity(CAPACITY + 3);
    for n in 0..CAPACITY + 3 {
        collection.push(cohort_assignment(&pool, tenant, &format!("m{n}")).await);
    }
    let foreign = cohort_assignment(&pool, Uuid::now_v7(), "foreign").await;
    let service = boot_counted_service(&db, nats.nats().await, "pod-s260", CAPACITY).await;
    let passport = passport_for(Uuid::now_v7(), tenant).to_header();

    let wanted = collection[2];
    let body = item(&service.base_url, &passport, wanted).await;
    assert_eq!(
        body["data"]["sampleCountedItem"]["id"],
        serde_json::json!(wanted.to_string()),
        "a visible row of a collection over window_capacity is fetched by its key: {body}"
    );
    assert!(
        ceilings_asked_for(tenant).is_empty(),
        "a one-key fetch never asks the store for the view's population"
    );
    assert_eq!(
        rows_read_of(&collection),
        1,
        "a one-key fetch reads the one row it answers, not the collection"
    );

    let body = item(&service.base_url, &passport, foreign).await;
    assert!(
        body["data"]["sampleCountedItem"].is_null(),
        "a row outside the caller's cohorts answers like an absent one: {body}"
    );
    let body = item(&service.base_url, &passport, Uuid::now_v7()).await;
    assert!(
        body["data"]["sampleCountedItem"].is_null(),
        "an absent key answers null: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

async fn raw_item(base_url: &str, passport: &str, field: &str, id: Uuid) -> serde_json::Value {
    let query = format!("query($id: UUID!) {{ {field} }}");
    let (status, body) = post_json(
        base_url,
        Some(passport),
        &query,
        serde_json::json!({ "id": id }),
    )
    .await;
    assert_eq!(status, 200, "the query answers over HTTP: {body}");
    body
}

const RAW_FIELDS: [&str; 2] = [
    "sampleCountedRawItem(id: $id) { id }",
    "sampleCountedRawItemJson(id: $id)",
];

#[tokio::test]
async fn s260_a_raw_one_key_fetch_reads_at_most_one_key_past_window_capacity() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let crowded = Uuid::now_v7();
    let mut crowd = Vec::with_capacity(CAPACITY + 3);
    for n in 0..CAPACITY + 3 {
        crowd.push(cohort_assignment(&pool, crowded, &format!("c{n}")).await);
    }
    let fitting = Uuid::now_v7();
    let mut fit = Vec::with_capacity(CAPACITY);
    for n in 0..CAPACITY {
        fit.push(cohort_assignment(&pool, fitting, &format!("f{n}")).await);
    }
    let service = boot_counted_service(&db, nats.nats().await, "pod-s260-raw", CAPACITY).await;

    let crowded_passport = passport_for(Uuid::now_v7(), crowded).to_header();
    for field in RAW_FIELDS {
        let body = raw_item(&service.base_url, &crowded_passport, field, crowd[0]).await;
        assert_eq!(
            body["errors"][0]["extensions"]["code"],
            serde_json::json!(WINDOW_TOO_LARGE_CODE),
            "a raw one-key fetch whose default window is over window_capacity cannot decide \
             membership and is refused like an attach: {body}"
        );
        assert!(body["data"].is_null(), "no view is rendered: {body}");
    }
    assert_eq!(
        ceilings_asked_for(crowded),
        vec![CAPACITY + 1, CAPACITY + 1],
        "each raw one-key fetch asks the store for window_capacity + 1 keys, never the whole \
         collection"
    );
    assert_eq!(
        rows_read_of(&crowd),
        0,
        "the refusal comes before any row is loaded or rendered"
    );

    let fitting_passport = passport_for(Uuid::now_v7(), fitting).to_header();
    let wanted = fit[1];
    let body = raw_item(&service.base_url, &fitting_passport, RAW_FIELDS[0], wanted).await;
    assert_eq!(
        body["data"]["sampleCountedRawItem"]["id"],
        serde_json::json!(wanted.to_string()),
        "a member of a default window at window_capacity is answered: {body}"
    );
    let body = raw_item(&service.base_url, &fitting_passport, RAW_FIELDS[1], wanted).await;
    assert_eq!(
        body["data"]["sampleCountedRawItemJson"]["id"],
        serde_json::json!(wanted.to_string()),
        "the JSON form answers the same member: {body}"
    );
    assert_eq!(
        ceilings_asked_for(fitting),
        vec![CAPACITY + 1, CAPACITY + 1],
        "an admitted raw fetch is bounded by the same ceiling"
    );
    let body = raw_item(
        &service.base_url,
        &fitting_passport,
        RAW_FIELDS[0],
        crowd[0],
    )
    .await;
    assert!(
        body["errors"].is_null() && body["data"]["sampleCountedRawItem"].is_null(),
        "a key outside the caller's default window answers null: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
