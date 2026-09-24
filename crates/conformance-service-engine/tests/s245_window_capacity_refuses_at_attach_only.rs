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
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments, SamplePrincipal};
use graphql_support::GraphqlWs;
use service_engine::delta::Delta;
use service_engine::error::AttachError;
use service_engine::impact::Dims;
use service_engine::metrics::{LABEL_OUTCOME, WINDOWS_OVER_CAPACITY_TOTAL};
use service_engine::registry::RenderRegistry;
use service_engine::session::WindowSpec;
use service_engine::{ViewProjector, WINDOW_TOO_LARGE_CODE, WindowSize};
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
async fn s245_an_attach_over_window_capacity_is_refused_and_one_at_capacity_attaches() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    for n in 0..CAPACITY + 3 {
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
        "a whole-collection window over window_capacity is refused at attach, and its populate \
         read {} of the {} matching rows, one past the capacity, got {refused:?}",
        CAPACITY + 1,
        CAPACITY + 3
    );
    assert_eq!(
        engine.live_sessions().await + engine.pending_sessions().await,
        0,
        "a refused attach leaves no session behind"
    );

    let capacity_size = page_size(CAPACITY);
    let mut narrowed = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentPage::head(capacity_size))],
        ))
        .await
        .expect("the same view narrowed by its arguments to window_capacity attaches");
    let reset = next_delta(&mut narrowed, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), CAPACITY);

    db.cleanup().await;
}

#[tokio::test]
async fn s245_an_attach_whose_last_window_is_over_capacity_renders_none_of_its_windows() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    for n in 0..=CAPACITY {
        assignment(&pool, home, &format!("m{n}")).await;
    }
    let spy = Spy::new();
    let mut registry = paged_registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::OrderedHead(2)),
        )
        .expect("the spy projector registers on the bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-capacity-two-windows").with_window_capacity(CAPACITY),
        registry,
    );

    let fitting = conformance_service_engine::sample::render::window(SpyAssignments::NAME, false);
    let refused = engine
        .attach(attach_request(
            &principal,
            vec![fitting, window(AssignmentPage::whole())],
        ))
        .await;
    assert!(
        matches!(refused, Err(AttachError::WindowTooLarge { .. })),
        "the second window, over window_capacity, refuses the whole attach, got {refused:?}"
    );
    assert_eq!(spy.populates(), 1, "the fitting first window was populated");
    assert_eq!(
        (spy.loads(), spy.projects()),
        (0, 0),
        "no window is loaded or projected before every window of the attach is admitted"
    );
    let metrics = engine.metrics();
    assert_eq!((metrics.loads, metrics.projections), (0, 0));
    assert_eq!(
        engine.live_sessions().await + engine.pending_sessions().await,
        0
    );

    db.cleanup().await;
}

fn page_size(size: usize) -> WindowSize {
    WindowSize::new(u32::try_from(size).expect("a small window")).expect("a positive window")
}

#[tokio::test]
async fn s245_a_size_far_above_capacity_is_refused_after_reading_one_key_past_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    for n in 0..CAPACITY + 3 {
        assignment(&pool, home, &format!("m{n}")).await;
    }
    let engine = runtime(
        &pool,
        render_config("pod-capacity-ceiling").with_window_capacity(CAPACITY),
        paged_registry(),
    );

    let widest = WindowSize::new(WindowSize::MAX).expect("the largest window size");
    let refused = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentPage::head(widest))],
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
        "a page far wider than window_capacity is refused, and its populate read {} of the {} \
         matching rows, one past the capacity, got {refused:?}",
        CAPACITY + 1,
        CAPACITY + 3
    );
    assert_eq!(
        engine.live_sessions().await + engine.pending_sessions().await,
        0
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s245_a_live_window_that_grows_past_capacity_stays_open_and_is_counted_once() {
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
            vec![window(AssignmentPage::head(page_size(CAPACITY + 5)))],
        ))
        .await
        .expect("a page wider than window_capacity attaches while its rows fit the capacity");
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
        let rest = drain(&mut stream).await;
        assert!(
            rest.is_empty(),
            "the page holds {} rows under a size of {}: none leaves, because a live window \
             repopulates by its size and never by the attach read, got {rest:?}",
            n + 1,
            CAPACITY + 5
        );
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
async fn s245_the_refusal_reaches_the_client_as_window_too_large() {
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
        base_config("se_s245", "pod-s245").with_window_capacity(1),
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
