mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::counted::{
    CountedAssignments, ceilings_asked_for, rows_read_of,
};
use conformance_service_engine::sample::graphql::{boot_counted_service, passport_for};
use conformance_service_engine::sample::render::*;
use graphql_support::post_json;
use service_engine::error::AttachError;
use service_engine::{ViewProjector, WINDOW_TOO_LARGE_CODE};
use sqlx::PgPool;
use uuid::Uuid;

const CAPACITY: usize = 4;
const SOON: Duration = Duration::from_secs(2);

async fn tenant_of(pool: &PgPool, rows: usize) -> (Uuid, Vec<Uuid>) {
    let tenant = Uuid::now_v7();
    let mut ids = Vec::with_capacity(rows);
    for n in 0..rows {
        ids.push(cohort_assignment(pool, tenant, &format!("m{n}")).await);
    }
    ids.sort();
    (tenant, ids)
}

#[tokio::test]
async fn s259_a_whole_collection_attach_over_capacity_asks_its_store_for_one_key_past_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let (crowded, crowded_ids) = tenant_of(&pool, CAPACITY + 3).await;
    let (fitting, fitting_ids) = tenant_of(&pool, CAPACITY).await;
    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(CountedAssignments))
        .expect("the counted cohort view registers on the bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-s259-attach").with_window_capacity(CAPACITY),
        registry,
    );

    let crowded_principal = member(&pool, Uuid::now_v7(), crowded).await;
    let refused = engine
        .attach(attach_request(
            &crowded_principal,
            vec![window(CountedAssignments::NAME, false)],
        ))
        .await;
    assert!(
        matches!(
            refused,
            Err(AttachError::WindowTooLarge {
                size,
                capacity: CAPACITY,
                ..
            }) if size == CAPACITY + 1
        ),
        "a whole-collection cohort window over window_capacity is refused with the one key past \
         the capacity that proves it, got {refused:?}"
    );
    assert_eq!(
        ceilings_asked_for(crowded),
        vec![CAPACITY + 1],
        "keys_in_cohorts is asked for window_capacity + 1 keys, so the store stops there and \
         never reads the {} rows of the collection",
        CAPACITY + 3
    );
    assert_eq!(
        rows_read_of(&crowded_ids),
        0,
        "no row is loaded for a refused attach"
    );

    let fitting_principal = member(&pool, Uuid::now_v7(), fitting).await;
    let mut stream = engine
        .attach(attach_request(
            &fitting_principal,
            vec![window(CountedAssignments::NAME, false)],
        ))
        .await
        .expect("a whole collection at window_capacity attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    let mut shown = assignment_ids(reset_views(&reset));
    shown.sort();
    assert_eq!(shown, fitting_ids, "the ceiling trims no admitted window");

    db.cleanup().await;
}

async fn ask(base_url: &str, passport: &str, field: &str) -> serde_json::Value {
    let query = format!("query {{ {field} }}");
    let (status, body) = post_json(base_url, Some(passport), &query, serde_json::json!({})).await;
    assert_eq!(status, 200, "the query answers over HTTP: {body}");
    body
}

#[tokio::test]
async fn s259_a_one_shot_window_fetch_over_capacity_is_window_too_large_before_any_row_is_read() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let (crowded, crowded_ids) = tenant_of(&pool, CAPACITY + 3).await;
    let (fitting, fitting_ids) = tenant_of(&pool, CAPACITY).await;
    let service = boot_counted_service(&db, nats.nats().await, "pod-s259", CAPACITY).await;

    let crowded_passport = passport_for(Uuid::now_v7(), crowded).to_header();
    for field in ["sampleCountedWindow { id }", "sampleCountedWindowJson"] {
        let body = ask(&service.base_url, &crowded_passport, field).await;
        assert_eq!(
            body["errors"][0]["extensions"]["code"],
            serde_json::json!(WINDOW_TOO_LARGE_CODE),
            "a one-shot window over window_capacity is refused with the code of an attach: {body}"
        );
        assert!(body["data"].is_null(), "no view is rendered: {body}");
    }
    assert_eq!(
        ceilings_asked_for(crowded),
        vec![CAPACITY + 1, CAPACITY + 1],
        "each one-shot fetch asks the store for window_capacity + 1 keys, as an attach does"
    );
    assert_eq!(
        rows_read_of(&crowded_ids),
        0,
        "the refusal comes before any row is loaded or rendered"
    );

    let fitting_passport = passport_for(Uuid::now_v7(), fitting).to_header();
    let body = ask(
        &service.base_url,
        &fitting_passport,
        "sampleCountedWindow { id }",
    )
    .await;
    let mut shown: Vec<Uuid> = body["data"]["sampleCountedWindow"]
        .as_array()
        .unwrap_or_else(|| panic!("a window at capacity answers its views: {body}"))
        .iter()
        .map(|view| {
            view["id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .expect("a view carries its id")
        })
        .collect();
    shown.sort();
    assert_eq!(
        shown, fitting_ids,
        "a window at capacity is fetched whole: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
