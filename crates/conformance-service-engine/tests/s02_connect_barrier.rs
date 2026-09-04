use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::gate::Gate;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use conformance_service_engine::sample::titles::TitleProjector;
use service_engine::delta::ErasedView;
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(300);

fn title_of(view: &ErasedView) -> String {
    view.view
        .decode::<serde_json::Value>()
        .expect("a rendered view decodes")
        .get("title")
        .and_then(|title| title.as_str())
        .expect("every sample view carries its title")
        .to_owned()
}

#[tokio::test]
async fn s02_a_connection_cancelled_after_it_went_live_is_reaped_with_its_stream() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let gate = Gate::times(2);
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone())
                .with_window(WindowMode::LiveQuery)
                .gated(gate.clone()),
        )
        .expect("the gated projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-cancelled").with_session_ttl(Duration::from_secs(3600)),
        registry,
    );

    let connecting = {
        let engine = engine.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { engine.attach(request).await })
    };

    gate.wait_until_inside().await;
    engine
        .render(vec![resource(&known, Dims::EMPTY)])
        .await
        .expect("a pass runs while the session is still assembling its snapshot");
    gate.release();

    gate.wait_until_inside().await;
    connecting.abort();
    let _ = connecting.await;

    assert_eq!(
        engine.live_sessions().await,
        1,
        "the cancelled connection had already gone live to replay what it held"
    );
    assert_eq!(
        engine.gc().await,
        1,
        "a connection cancelled after it went live is reaped with the stream it never returned"
    );
    assert_eq!(
        engine.live_sessions().await,
        0,
        "a live session with no stream is never immortal"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s02_a_burst_beyond_the_held_bound_opens_the_session_on_the_truth_the_database_holds() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let gate = Gate::new();
    let mut registry = registry();
    registry
        .register_projector(TitleProjector::new(Arc::new(AtomicUsize::new(0))))
        .expect("the title projector registers on a bound noun");
    registry
        .register_projector(SpyAssignments::new(spy.clone()).gated(gate.clone()))
        .expect("the gated projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-burst").with_max_held_impacts(2),
        registry,
    );

    let connecting = {
        let engine = engine.clone();
        let request = attach_request(
            &principal,
            vec![
                window(TitleProjector::NAME, false),
                window(SpyAssignments::NAME, false),
            ],
        );
        tokio::spawn(async move { engine.attach(request).await })
    };

    gate.wait_until_inside().await;
    retitle(&pool, known, "beta").await;
    for _ in 0..3 {
        engine
            .render(vec![resource(&known, Dims::EMPTY)])
            .await
            .expect("a pass runs while the session is still assembling its snapshot");
    }
    gate.release();

    let mut stream = connecting
        .await
        .expect("the attach task completes")
        .expect("the session attaches");

    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(
        first.revision().get(),
        1,
        "the Reset is still the first frame the client sees"
    );
    let views = reset_views(&first);
    assert_eq!(views.len(), 2, "both windows are carried by the Reset");
    for view in views {
        assert_eq!(
            title_of(view),
            "beta",
            "a burst past the held bound opens the session on what the database holds now, \
             never on the half-stale snapshot it started from"
        );
    }
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the burst was upgraded to a Reset, so nothing is replayed behind it"
    );

    db.cleanup().await;
}
