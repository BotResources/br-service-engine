mod graphql_support;

use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::metrics_probe;
use conformance_service_engine::sample::graphql::{
    base_config, boot_graphql_service_with, passport_for,
};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments, SamplePrincipal};
use graphql_support::GraphqlWs;
use service_engine::delta::Delta;
use service_engine::error::AttachError;
use service_engine::impact::Dims;
use service_engine::metrics::{LABEL_OUTCOME, WINDOWS_OVER_CAPACITY_TOTAL};
use service_engine::registry::RenderRegistry;
use service_engine::session::WindowSpec;
use service_engine::{ViewProjector, WINDOW_TOO_LARGE_CODE};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const CAPACITY: usize = 4;

fn window(page: AssignmentPage) -> WindowSpec {
    WindowSpec::view::<PagedAssignments>(&page, false).expect("the window arguments encode")
}

fn paged_registry() -> RenderRegistry<SamplePrincipal> {
    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers on the bound noun");
    registry
}

#[tokio::test]
async fn s163_an_attach_over_window_capacity_is_refused_and_one_at_capacity_attaches() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    for n in 0..=CAPACITY {
        assignment(&pool, home, &format!("m{n}")).await;
    }
    let engine = runtime(
        &pool,
        render_config("pod-capacity-refused").with_window_capacity(CAPACITY),
        paged_registry(),
    );

    let refused = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentPage::whole())],
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
        "a window one key over window_capacity is refused at attach, got {refused:?}"
    );
    assert_eq!(
        engine.live_sessions().await + engine.pending_sessions().await,
        0,
        "a refused attach leaves no session behind"
    );

    let mut narrowed = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentPage::head(CAPACITY as i64))],
        ))
        .await
        .expect("the same view narrowed by its arguments to window_capacity attaches");
    let reset = next_delta(&mut narrowed, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), CAPACITY);

    db.cleanup().await;
}

#[tokio::test]
async fn s163_a_live_window_that_grows_past_capacity_stays_open_and_is_counted_once() {
    let probe = metrics_probe::install();
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    for n in 0..CAPACITY {
        assignment(&pool, home, &format!("m{n}")).await;
    }
    let engine = runtime(
        &pool,
        render_config("pod-capacity-kept").with_window_capacity(CAPACITY),
        paged_registry(),
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentPage::whole())],
        ))
        .await
        .expect("a window exactly at window_capacity attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), CAPACITY);
    let kept_before = probe.labelled_total(WINDOWS_OVER_CAPACITY_TOTAL, LABEL_OUTCOME, "kept");

    for (n, revision) in [(CAPACITY, 2), (CAPACITY + 1, 3)] {
        let grown = assignment(&pool, home, &format!("m{n}")).await;
        engine
            .render(vec![resource(&grown, Dims::ALL)])
            .await
            .expect("the pass runs");
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("the live window keeps delivering past window_capacity");
        assert!(
            matches!(delta, Delta::Upsert { .. }),
            "a window past capacity is never ended nor reset for its size, got {delta:?}"
        );
        assert_eq!(delta.revision().get(), revision);
        assert_eq!(assignment_ids(&[upserted(&delta).clone()]), vec![grown]);
        assert_eq!(engine.live_sessions().await, 1, "the session stays live");
    }
    assert_eq!(
        probe.labelled_total(WINDOWS_OVER_CAPACITY_TOTAL, LABEL_OUTCOME, "kept") - kept_before,
        1,
        "crossing window_capacity is counted once, not once per key past it"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s163_the_refusal_reaches_the_client_as_window_too_large() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, tenant).await;
    assignment(&pool, tenant, "first").await;
    assignment(&pool, tenant, "second").await;

    let service = boot_graphql_service_with(
        &db,
        nats.nats().await,
        base_config("se_s163", "pod-s163").with_window_capacity(1),
    )
    .await;
    let passport = passport_for(user, tenant).to_header();

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    ws.subscribe(
        "1",
        "subscription { sampleAssignments { __typename ... on ResetPayload { revision } } }",
    )
    .await;
    let payload = ws
        .next_payload(Duration::from_secs(15))
        .await
        .expect("the refused attach answers the operation");
    assert_eq!(
        payload["errors"][0]["extensions"]["code"],
        serde_json::json!(WINDOW_TOO_LARGE_CODE),
        "an over-capacity attach is refused with its own code, never INTERNAL: {payload}"
    );
    assert!(
        payload["data"].is_null(),
        "no view is rendered for a refused window: {payload}"
    );
    ws.expect_complete(Duration::from_secs(5)).await;

    service.shutdown().await;
    db.cleanup().await;
}
