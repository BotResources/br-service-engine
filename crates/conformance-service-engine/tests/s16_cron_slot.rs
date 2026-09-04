use std::time::Duration;

use chrono::TimeZone;
use conformance_service_engine::infra::TestDb;
use service_engine::housekeeping::leader::{
    SlotName, advisory_key, claim_slot_at, complete_slot, renew_slot, sweep_completed_slots,
};
use service_engine::name::{JobName, PodId, RelayName};
use service_engine::time::Timestamp;
use sqlx::PgPool;

const JOB: &str = "nightly";
const LEASE: Duration = Duration::from_secs(30);
const BEAT: Duration = Duration::from_millis(100);

#[tokio::test]
async fn s16_a_cron_slot_is_named_by_its_fire_time_and_runs_once_across_two_skewed_pods() {
    let db = TestDb::fresh().await;
    let one = PodId::new("se-cron-0").expect("a valid pod id");
    let two = PodId::new("se-cron-1").expect("a valid pod id");
    let fire = at(1_800_000_000);

    let mut winner = {
        let mut conn = db.app_pool().acquire().await.expect("a connection");
        claim_slot_at(
            &mut conn,
            SlotName::Cron(JobName::from_static(JOB)),
            fire,
            &one,
            LEASE,
        )
        .await
        .expect("claim the fire time")
        .expect("the first pod to see the slot takes it")
    };
    assert_eq!(
        winner.slot(),
        fire,
        "the slot is the fire time the schedule computed, not a bin of the database clock"
    );

    let mut conn = db.app_pool().acquire().await.expect("a connection");
    let loser = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static(JOB)),
        fire,
        &two,
        LEASE,
    )
    .await
    .expect("the second pod asks for the same slot");
    assert!(
        loser.is_none(),
        "two pods computing the same fire time must not both run it"
    );
    drop(conn);

    for _ in 0..2 {
        tokio::time::sleep(BEAT).await;
        assert_eq!(
            idle_in_transaction(db.app_pool()).await,
            0,
            "a job outliving a beat must hold no transaction open while it runs"
        );
        let mut conn = db.app_pool().acquire().await.expect("a connection");
        assert!(
            renew_slot(&mut conn, &mut winner, LEASE)
                .await
                .expect("renew the lease while the job runs")
        );
    }

    let mut conn = db.app_pool().acquire().await.expect("a connection");
    assert!(
        complete_slot(&mut conn, &winner)
            .await
            .expect("mark the slot done")
    );
    let replay = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static(JOB)),
        fire,
        &two,
        LEASE,
    )
    .await
    .expect("a pod that sees the slot after it completed");
    assert!(
        replay.is_none(),
        "a completed slot is never run a second time, however late another pod looks at it"
    );

    drop(conn);
    assert_eq!(rows(db.app_pool(), "cron:nightly").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_missed_slot_is_claimable_after_the_fact_and_a_dead_holder_is_taken_over() {
    let db = TestDb::fresh().await;
    let one = PodId::new("se-cron-0").expect("a valid pod id");
    let two = PodId::new("se-cron-1").expect("a valid pod id");

    let missed = service_engine::time::now() - chrono::Duration::hours(1);
    let mut conn = db.app_pool().acquire().await.expect("a connection");
    let caught_up = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static("catchup")),
        missed,
        &one,
        LEASE,
    )
    .await
    .expect("claim a fire time that passed while every pod was down")
    .expect("a slot missed while nobody was up is still runnable");
    assert_eq!(caught_up.slot(), missed);
    assert!(
        complete_slot(&mut conn, &caught_up)
            .await
            .expect("mark the missed slot done")
    );

    let short = Duration::from_millis(300);
    let fire = at(1_800_000_600);
    let abandoned = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static(JOB)),
        fire,
        &one,
        short,
    )
    .await
    .expect("claim the slot")
    .expect("the first pod takes it");
    assert_eq!(abandoned.pod(), &one);
    assert!(
        claim_slot_at(
            &mut conn,
            SlotName::Cron(JobName::from_static(JOB)),
            fire,
            &two,
            LEASE
        )
        .await
        .expect("the other pod asks while the lease still holds")
        .is_none(),
        "a live lease is not stolen"
    );

    tokio::time::sleep(short + Duration::from_millis(150)).await;
    let taken_over = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static(JOB)),
        fire,
        &two,
        LEASE,
    )
    .await
    .expect("the other pod asks once the lease expired")
    .expect("a pod that died mid-run must not lose the slot forever");
    assert_eq!(taken_over.pod(), &two);
    assert!(
        complete_slot(&mut conn, &taken_over)
            .await
            .expect("mark the retried slot done")
    );
    assert!(
        !complete_slot(&mut conn, &abandoned)
            .await
            .expect("the dead holder comes back and tries to complete"),
        "the pod that lost its lease must not mark the slot its successor already ran"
    );

    drop(conn);
    assert_eq!(rows(db.app_pool(), "cron:nightly").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_relay_and_a_cron_job_of_the_same_name_never_share_a_slot() {
    let db = TestDb::fresh().await;
    let pod = PodId::new("se-cron-0").expect("a valid pod id");
    let fire = at(1_800_001_200);
    let mut conn = db.app_pool().acquire().await.expect("a connection");

    let job = claim_slot_at(
        &mut conn,
        SlotName::Cron(JobName::from_static(JOB)),
        fire,
        &pod,
        LEASE,
    )
    .await
    .expect("claim the job's slot")
    .expect("the job takes its slot");
    let relay = claim_slot_at(
        &mut conn,
        SlotName::Relay(RelayName::from_static(JOB)),
        fire,
        &pod,
        LEASE,
    )
    .await
    .expect("claim the relay's slot")
    .expect("a relay of the same name must not be skipped by the job's claim");

    assert_eq!(job.qualified_name(), "cron:nightly");
    assert_eq!(relay.qualified_name(), "relay:nightly");
    assert_ne!(
        advisory_key(&SlotName::Cron(JobName::from_static(JOB))),
        advisory_key(&SlotName::Relay(RelayName::from_static(JOB)))
    );
    assert_eq!(rows(db.app_pool(), "cron:nightly").await, 1);
    assert_eq!(rows(db.app_pool(), "relay:nightly").await, 1);

    for lease in [&job, &relay] {
        assert!(
            complete_slot(&mut conn, lease)
                .await
                .expect("mark the slot done")
        );
    }
    let swept = sweep_completed_slots(&mut conn, Duration::ZERO)
        .await
        .expect("the beat collects completed slots");
    assert_eq!(swept, 2, "both completed slots are collected");
    drop(conn);
    assert_eq!(rows(db.app_pool(), "cron:nightly").await, 0);

    db.cleanup().await;
}

fn at(secs: i64) -> Timestamp {
    Timestamp::from_utc(
        chrono::Utc
            .timestamp_opt(secs, 0)
            .single()
            .expect("a representable fire time"),
    )
}

async fn rows(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.leader_slot WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("count the slot rows")
}

async fn idle_in_transaction(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity \
          WHERE datname = current_database() AND state = 'idle in transaction'",
    )
    .fetch_one(pool)
    .await
    .expect("read the backends of this database")
}
