use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::gate::Gate;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use service_engine::config::EngineConfig;
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::runtime::SessionRuntime;
use sqlx::PgPool;
use uuid::Uuid;

use conformance_service_engine::sample::SamplePrincipal;
use std::sync::Arc;

const SOON: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(300);

fn connecting(
    pool: &PgPool,
    config: EngineConfig,
) -> (Arc<SessionRuntime<SamplePrincipal>>, Arc<Gate>, Arc<Spy>) {
    let spy = Spy::new();
    let gate = Gate::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone())
                .with_window(WindowMode::OrderedHead(50))
                .gated(gate.clone()),
        )
        .expect("the gated projector registers on a bound noun");
    (runtime(pool, config, registry), gate, spy)
}

#[tokio::test]
async fn s02_an_impact_committed_during_the_snapshot_is_neither_lost_nor_duplicated() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let (engine, gate, _spy) = connecting(&pool, render_config("pod-connect"));

    let connecting = {
        let engine = engine.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { engine.attach(request).await })
    };

    gate.wait_until_inside().await;
    let late = assignment(&pool, home, "beta").await;
    let report = engine
        .render(vec![resource(&late, Dims::EMPTY)])
        .await
        .expect("a pass runs while the session is still assembling its snapshot");
    assert_eq!(
        report.sessions, 0,
        "a session that has not enqueued its Reset receives no delta from a concurrent pass"
    );
    gate.release();

    let mut stream = connecting
        .await
        .expect("the attach task completes")
        .expect("the session attaches");

    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert!(
        matches!(first, Delta::Reset { .. }),
        "the Reset is the first frame even when a pass ran during the snapshot, got {first:?}"
    );
    assert_eq!(first.revision().get(), 1);
    assert_eq!(
        assignment_ids(reset_views(&first)),
        vec![known],
        "the snapshot holds what the window read, and nothing the pass staged after it"
    );

    let held = next_delta(&mut stream, SOON)
        .await
        .expect("the impact held during the snapshot is replayed, never discarded");
    assert_eq!(held.revision().get(), 2);
    assert_eq!(
        upserted(&held).key.decode::<Uuid>().unwrap(),
        late,
        "the row committed during the snapshot reaches the session exactly once"
    );
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the held impact is replayed once, not twice"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s02_an_impact_the_snapshot_already_saw_yields_no_second_delta() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let (engine, gate, _spy) = connecting(&pool, render_config("pod-seen"));

    let connecting = {
        let engine = engine.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { engine.attach(request).await })
    };

    gate.wait_until_inside().await;
    engine
        .render(vec![resource(&known, Dims::EMPTY)])
        .await
        .expect("a pass runs on a row the snapshot has already read");
    gate.release();

    let mut stream = connecting
        .await
        .expect("the attach task completes")
        .expect("the session attaches");

    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(first.revision().get(), 1);
    assert_eq!(assignment_ids(reset_views(&first)), vec![known]);
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "an impact the snapshot already saw diffs to nothing, so it is never delivered twice"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s02_a_connection_abandoned_before_its_reset_is_reaped_after_the_session_ttl() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let ttl = Duration::from_millis(200);
    let (engine, gate, _spy) =
        connecting(&pool, render_config("pod-abandoned").with_session_ttl(ttl));

    let connecting = {
        let engine = engine.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { engine.attach(request).await })
    };
    gate.wait_until_inside().await;
    connecting.abort();
    let _ = connecting.await;

    assert_eq!(
        engine.live_sessions().await,
        0,
        "a connection that never enqueued its Reset is not a live session"
    );
    assert_eq!(
        engine.gc().await,
        0,
        "a pending connection is given its whole ttl before it is reaped"
    );
    tokio::time::sleep(ttl * 2).await;
    assert_eq!(
        engine.gc().await,
        1,
        "a connection abandoned before it went live is collected once its ttl has passed"
    );
    assert_eq!(engine.gc().await, 0);

    db.cleanup().await;
}

#[tokio::test]
async fn s02_a_dropped_stream_reaps_its_session_on_the_next_pass() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-dropped"), registry);

    let stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    assert_eq!(engine.live_sessions().await, 1);
    drop(stream);

    retitle(&pool, known, "renamed").await;
    let report = engine
        .render(vec![resource(&known, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.sessions, 0,
        "a session whose stream was dropped is reaped before the pass renders anything for it"
    );
    assert_eq!(engine.live_sessions().await, 0);

    db.cleanup().await;
}
