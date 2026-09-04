#[allow(dead_code)]
mod engine_twin;

use chrono::Duration as ChronoDuration;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::Assignment;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine};
use service_engine::wire::{Noun, encode_key};
use uuid::Uuid;

const CHANNEL: &str = "se_s09_engine";

#[tokio::test]
async fn s09_engine_a_due_boundary_fires_once_on_the_beat_as_a_resource_change() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;
    let other = assignment(&pool, home, "gamma").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s09"),
        SpyAssignments::new(Spy::new()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    retitle(&pool, subject, "beta").await;

    let now = service_engine::time::now();
    let mut tx = pool.begin().await.expect("a write transaction");
    transport
        .schedule_in(
            &mut tx,
            Assignment::NAME,
            encode_key::<Assignment>(&subject).unwrap(),
            now - ChronoDuration::seconds(1),
        )
        .await
        .expect("register a boundary that is already due");
    transport
        .schedule_in(
            &mut tx,
            Assignment::NAME,
            encode_key::<Assignment>(&other).unwrap(),
            now + ChronoDuration::hours(1),
        )
        .await
        .expect("register a boundary in the future");
    tx.commit()
        .await
        .expect("commit: registering a boundary notifies nothing");

    let fired = next_delta(&mut stream, SOON)
        .await
        .expect("the due boundary fires on the beat and reaches the pod as a ResourceChanged");
    assert_eq!(fired.revision().get(), 2);
    let title = upserted(&fired).view.decode::<serde_json::Value>().unwrap()["title"]
        .as_str()
        .expect("a title")
        .to_string();
    assert_eq!(
        title, "beta",
        "the fired boundary re-rendered the row from the database"
    );

    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.scheduled_impact")
        .fetch_one(&pool)
        .await
        .expect("read the boundary table");
    assert_eq!(
        left, 1,
        "the fired boundary is deleted, the future one is not"
    );

    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "a claimed boundary never fires twice from the same clock"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
