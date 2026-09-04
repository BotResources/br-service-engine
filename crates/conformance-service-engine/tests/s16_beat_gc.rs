use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::sample::{BusySampleRelay, SampleCronJob, cron_runs};
use service_engine::cron::Schedule;
use service_engine::housekeeping::beat::Beat;
use service_engine::housekeeping::leader::{SlotName, claim_slot_at};
use service_engine::name::{JobName, PodId, RelayName};
use service_engine::time;
use sqlx::PgPool;
use tokio::sync::Notify;

const BEAT: Duration = Duration::from_millis(100);
const RETENTION: Duration = Duration::from_millis(300);

#[tokio::test]
async fn s16_the_beat_collects_the_slot_rows_of_a_job_that_already_ran() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let config = engine_config("se_s16_gc", "se-cron-0").with_beat(BEAT);

    let mut beat = Beat::from_config(&config)
        .expect("a beat from the engine config")
        .with_slot_retention(RETENTION)
        .with_gc_interval(Duration::ZERO);
    beat.cron()
        .register(SampleCronJob::new(
            "collected",
            Schedule::EveryBeats(1),
            "se-cron-0",
        ))
        .expect("a job registers");

    let abandoned = claim_slot_at(
        &mut pool.acquire().await.expect("a connection"),
        SlotName::Cron(JobName::from_static("crashed")),
        time::now(),
        &PodId::new("se-cron-9").expect("a valid pod id"),
        Duration::from_millis(50),
    )
    .await
    .expect("claim a slot for a pod that will never come back")
    .expect("the slot is free");
    assert_eq!(slots(&pool, "cron:crashed").await, 1);

    let mut completed_rows = 0u64;
    let mut abandoned_rows = 0u64;
    let mut ran = 0i64;
    let mut started = 0usize;
    for _ in 0..40 {
        let round = beat.tick(&pool).await;
        assert!(round.gc.ran, "a zero interval sweeps on every beat");
        started += round.cron.ran;
        completed_rows += round.gc.completed_slots;
        abandoned_rows += round.gc.abandoned_slots;
        ran = ran.max(cron_runs(&pool, "collected").await);
        if completed_rows >= 1 && abandoned_rows >= 1 {
            break;
        }
        tokio::time::sleep(BEAT).await;
    }

    assert!(ran >= 1, "the beat never ran the job it was given");
    assert!(
        completed_rows >= 1,
        "the beat collects the slot rows of a job that already ran"
    );
    assert!(
        abandoned_rows >= 1,
        "a slot claimed by a pod that never came back is collected once its lease is long expired"
    );
    assert_eq!(
        abandoned.qualified_name(),
        "cron:crashed",
        "the collected row is the one the dead pod claimed"
    );
    assert_eq!(
        slots(&pool, "cron:crashed").await,
        0,
        "an abandoned slot row must not accumulate forever"
    );

    for _ in 0..4 {
        let round = beat.tick(&pool).await;
        started += round.cron.ran;
        tokio::time::sleep(BEAT).await;
    }
    assert!(
        cron_runs(&pool, "collected").await <= i64::try_from(started).unwrap(),
        "a job must never run more times than it claimed a slot; collecting a slot row the cron \
         layer would still claim is exactly what would make it"
    );

    db.cleanup().await;
}

async fn slots(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.leader_slot WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("count the slot rows")
}

#[tokio::test]
async fn s16_a_slot_whose_row_the_collector_already_took_is_never_run_a_second_time() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let config = engine_config("se_s16_horizon", "se-cron-0").with_beat(BEAT);

    let mut beat = Beat::from_config(&config)
        .expect("a beat from the engine config")
        .with_slot_retention(Duration::from_secs(1))
        .with_gc_interval(Duration::ZERO);
    beat.cron()
        .register(SampleCronJob::new(
            "sparse",
            Schedule::Every {
                period: Duration::from_secs(3),
                anchor: time::now(),
            },
            "se-cron-0",
        ))
        .expect("a job whose period outlasts the slot retention registers");

    let mut collected = 0u64;
    let mut expired = 0usize;
    for _ in 0..25 {
        let round = beat.tick(&pool).await;
        collected += round.gc.completed_slots;
        expired += round.cron.expired;
        tokio::time::sleep(BEAT).await;
    }

    assert_eq!(
        cron_runs(&pool, "sparse").await,
        1,
        "the job fires once per three-second period; a slot whose row the collector took must \
         not become due again"
    );
    assert!(
        collected >= 1,
        "the collector really took the completed slot row inside the run"
    );
    assert!(
        expired >= 1,
        "past the retention the cron layer stops naming the slot at all, because its record is \
         gone and it could no longer tell that it ran"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_beat_whose_relays_keep_asking_for_another_round_still_stops_when_it_is_told_to() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let config = engine_config("se_s16_busy", "se-beat-0")
        .with_beat(Duration::from_secs(5))
        .with_lease(Duration::from_secs(30));

    let mut beat = Beat::from_config(&config).expect("a beat from the engine config");
    let busy = Arc::new(BusySampleRelay::new(RelayName::from_static("busy")));
    beat.relays()
        .register_erased(busy.clone())
        .expect("the busy relay registers");

    let shutdown = Arc::new(Notify::new());
    let after_pass = Arc::new(Notify::new());
    let running = tokio::spawn(beat.run(pool, shutdown.clone(), after_pass));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        busy.drains() > 1,
        "the relay must really be asking the beat back for another round, got {} drain(s)",
        busy.drains()
    );
    assert!(
        !running.is_finished(),
        "the beat is still looping on the relay that never empties"
    );

    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("a beat asked to stop must stop, however much work its relays keep reporting")
        .expect("the beat task ended cleanly");

    db.cleanup().await;
}
