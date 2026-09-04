use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::listener::{engine_config, pool_named, terminate_and_wait};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use service_engine::delta::Delta;
use service_engine::impact::{Dims, Impact};
use service_engine::transport::{ImpactTransport, PgListenNotify};
use sqlx::PgPool;
use tokio::sync::Notify;
use uuid::Uuid;

const CHANNEL: &str = "se_s13_sessions_impact";
const LISTENER: &str = "se_s13_sessions_listener";
const QUERY_CHANNEL: &str = "se_s13_query_impact";
const QUERY_LISTENER: &str = "se_s13_query_listener";
const SOON: Duration = Duration::from_secs(15);

async fn stage(pool: &PgPool, transport: &PgListenNotify, id: Uuid, title: &str) -> Impact {
    let impact = resource(&id, Dims::EMPTY);
    let mut tx = pool.begin().await.expect("open a write transaction");
    sqlx::query("UPDATE sample_assignment SET title = $1 WHERE id = $2")
        .bind(title)
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("retitle inside the write transaction");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact inside the same transaction");
    tx.commit().await.expect("commit");
    impact
}

fn titles(delta: &Delta) -> Vec<String> {
    reset_views(delta)
        .iter()
        .map(|view| {
            view.view.decode::<serde_json::Value>().unwrap()["title"]
                .as_str()
                .expect("a title")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn s13_a_transport_reconnect_resets_every_session_and_resumes() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let first = assignment(&pool, home, "alpha").await;
    let second = assignment(&pool, home, "beta").await;

    let config = engine_config(CHANNEL, "pod-reconnect");
    let transport =
        PgListenNotify::connect(pool_named(&db, db.app_role(), LISTENER).await, &config)
            .await
            .expect("the listener is established");

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, config, registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(opening.revision().get(), 1);

    let stop = Arc::new(Notify::new());
    let running = tokio::spawn(engine.clone().run(transport.listen(), stop.clone()));

    stage(&pool, &transport, first, "before the reconnect").await;
    let before = next_delta(&mut stream, SOON)
        .await
        .expect("the pod renders what it heard");
    assert_eq!(before.revision().get(), 2);
    assert!(matches!(before, Delta::Upsert { .. }));

    sqlx::query("UPDATE sample_assignment SET title = $1 WHERE id = $2")
        .bind("changed while deaf")
        .bind(second)
        .execute(&pool)
        .await
        .expect("a row changes while the listener is down");
    terminate_and_wait(&db, LISTENER).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("a transport reconnect resets every session rather than leaving it stale");
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "a reconnect is a possible gap, so the session is re-snapshotted, got {reset:?}"
    );
    assert_eq!(reset.revision().get(), 3);
    let mut seen = titles(&reset);
    seen.sort();
    assert_eq!(
        seen,
        vec![
            "before the reconnect".to_string(),
            "changed while deaf".to_string()
        ],
        "the Reset is read from PostgreSQL, so it carries the change the pod never heard"
    );
    assert_eq!(engine.metrics().transport_reconnects, 1);
    assert_eq!(engine.metrics().resets, 1);

    stage(&pool, &transport, first, "after the reconnect").await;
    let resumed = next_delta(&mut stream, SOON)
        .await
        .expect("delivery resumes on the reconnected listener");
    assert_eq!(
        resumed.revision().get(),
        4,
        "the revision keeps running across the reconnect"
    );
    assert!(matches!(resumed, Delta::Upsert { .. }));

    stop.notify_one();
    running.await.expect("the render task stops");
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the stream ends explicitly on shutdown, never on a reconnect"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s13_a_query_window_keeps_what_it_discovered_across_a_reconnect() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let watched = assignment(&pool, home, "alpha").await;

    let config = engine_config(QUERY_CHANNEL, "pod-query");
    let transport = PgListenNotify::connect(
        pool_named(&db, db.app_role(), QUERY_LISTENER).await,
        &config,
    )
    .await
    .expect("the listener is established");

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_window(WindowMode::LiveQuery))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, config, registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert!(
        reset_views(&opening).is_empty(),
        "a Query window has nothing to enumerate until an impact discovers it"
    );

    let stop = Arc::new(Notify::new());
    let running = tokio::spawn(engine.clone().run(transport.listen(), stop.clone()));

    stage(&pool, &transport, watched, "discovered").await;
    let discovered = next_delta(&mut stream, SOON)
        .await
        .expect("the predicate discovers the key the impact named");
    assert_eq!(upserted(&discovered).key.decode::<Uuid>().unwrap(), watched);

    terminate_and_wait(&db, QUERY_LISTENER).await;
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("a reconnect resets the session");
    assert_eq!(
        assignment_ids(reset_views(&reset)),
        vec![watched],
        "a re-snapshot re-renders what the Query window discovered instead of emptying it"
    );

    stop.notify_one();
    running.await.expect("the render task stops");
    db.cleanup().await;
}
