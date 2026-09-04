use std::sync::Arc;
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::{
    engine_config, expect_event, next_event, pool_named,
};
use conformance_service_engine::sample::Assignment;
use service_engine::housekeeping::beat::Beat;
use service_engine::impact::{Dims, Impact, TransportEvent};
use service_engine::transport::{ImpactTransport, PgListenNotify};
use service_engine::wire::{Noun, encode_key};
use sqlx::Row;
use uuid::Uuid;

const HEARD_WITHIN: Duration = Duration::from_secs(10);
const SILENCE: Duration = Duration::from_millis(300);

#[tokio::test]
async fn s09_a_scheduled_boundary_is_claimed_staged_and_deleted_in_one_transaction() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s09_scheduled").await;
    let transport = PgListenNotify::connect(pool, &engine_config("se_s09_scheduled", "pod-a"))
        .await
        .expect("the listener is established");
    let mut stream = transport.listen();

    let due = Uuid::now_v7();
    let later = Uuid::now_v7();
    let now = service_engine::time::now();
    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a write transaction");
    transport
        .schedule_in(
            &mut tx,
            Assignment::NAME,
            encode_key::<Assignment>(&due).unwrap(),
            now - ChronoDuration::seconds(1),
        )
        .await
        .expect("register a boundary that is already due");
    transport
        .schedule_in(
            &mut tx,
            Assignment::NAME,
            encode_key::<Assignment>(&later).unwrap(),
            now + ChronoDuration::hours(1),
        )
        .await
        .expect("register a boundary in the future");
    tx.commit().await.expect("commit");

    assert!(
        next_event(&mut stream, SILENCE).await.is_none(),
        "registering a boundary notifies nothing"
    );

    assert_eq!(
        transport
            .fire_due(100)
            .await
            .expect("claim every boundary whose time has passed"),
        1,
        "fire_due reports how many boundaries it staged; the impacts themselves reach every pod \
         over LISTEN, this one included"
    );
    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(vec![
            Impact::resource::<Assignment>(&due, Dims::EMPTY).unwrap()
        ]),
        "a due boundary reaches every pod as an ordinary ResourceChanged"
    );

    let left: Vec<Uuid> = sqlx::query("SELECT id FROM service_engine.scheduled_impact")
        .fetch_all(db.app_pool())
        .await
        .expect("read the boundary table")
        .iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();
    assert_eq!(
        left.len(),
        1,
        "the fired boundary is deleted, the future one is not"
    );

    assert_eq!(
        transport.fire_due(100).await.unwrap(),
        0,
        "a claimed boundary never fires twice from the same clock"
    );

    let usage = transport
        .queue_usage()
        .await
        .expect("the notification queue usage is observable");
    assert!((0.0..=1.0).contains(&usage));

    db.cleanup().await;
}

#[tokio::test]
async fn s09_a_boundary_fires_once_on_the_beat_across_two_engines_on_one_database() {
    let db = TestDb::fresh().await;
    let config = engine_config("se_s09_beat", "pod-a");
    let one = Arc::new(
        PgListenNotify::connect(
            pool_named(&db, db.app_role(), "se_s09_beat_a").await,
            &config,
        )
        .await
        .expect("the first pod listens"),
    );
    let two = Arc::new(
        PgListenNotify::connect(
            pool_named(&db, db.app_role(), "se_s09_beat_b").await,
            &engine_config("se_s09_beat", "pod-b"),
        )
        .await
        .expect("the second pod listens"),
    );
    let mut heard_by_one = one.listen();
    let mut heard_by_two = two.listen();

    let mut first = Beat::from_config(&config)
        .expect("a beat from the engine config")
        .with_transport(one.clone());
    let mut second = Beat::from_config(&engine_config("se_s09_beat", "pod-b"))
        .expect("a beat from the engine config")
        .with_transport(two.clone());

    let due = Uuid::now_v7();
    let mut tx = db.app_pool().begin().await.expect("the write transaction");
    one.schedule_in(
        &mut tx,
        Assignment::NAME,
        encode_key::<Assignment>(&due).unwrap(),
        service_engine::time::now() - ChronoDuration::seconds(1),
    )
    .await
    .expect("register a boundary that is already due");
    tx.commit().await.expect("commit");

    assert!(
        next_event(&mut heard_by_one, SILENCE).await.is_none(),
        "registering a boundary notifies nothing, on either pod"
    );
    assert!(next_event(&mut heard_by_two, SILENCE).await.is_none());

    let rounds = [
        first.tick(db.app_pool()).await,
        second.tick(db.app_pool()).await,
    ];
    let fired: usize = rounds.iter().map(|round| round.scheduled.fired).sum();
    assert_eq!(
        fired, 1,
        "two engines beating on one database claim the boundary once between them"
    );

    let expected = TransportEvent::Impacts(vec![
        Impact::resource::<Assignment>(&due, Dims::EMPTY).unwrap(),
    ]);
    assert_eq!(
        expect_event(&mut heard_by_one, HEARD_WITHIN).await,
        expected,
        "the pod that fired hears itself"
    );
    assert_eq!(
        expect_event(&mut heard_by_two, HEARD_WITHIN).await,
        expected,
        "the pod that did not fire hears it too"
    );

    for _ in 0..3 {
        first.tick(db.app_pool()).await;
        second.tick(db.app_pool()).await;
    }
    assert!(
        next_event(&mut heard_by_one, SILENCE).await.is_none(),
        "a boundary is deleted in the transaction that stages it, so no later beat re-fires it"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM service_engine.scheduled_impact")
            .fetch_one(db.app_pool())
            .await
            .expect("read the boundary table"),
        0
    );

    db.cleanup().await;
}
