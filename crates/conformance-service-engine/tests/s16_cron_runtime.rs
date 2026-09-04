use std::time::Duration;

use chrono::Duration as ChronoDuration;
use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::pool_named;
use conformance_service_engine::sample::{
    SampleCronJob, claimed_slots, completed_slots, cron_pods, cron_runs,
};
use service_engine::cron::{CronExpr, Schedule};
use service_engine::housekeeping::cron::CronRuntime;
use service_engine::name::PodId;
use service_engine::time;
use sqlx::PgPool;

const BEAT: Duration = Duration::from_millis(100);
const LEASE: Duration = Duration::from_millis(600);
const BEATS: usize = 8;

fn runtime(pod: &str) -> CronRuntime {
    CronRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_beat(BEAT)
        .with_lease(LEASE)
}

#[tokio::test]
async fn s16_a_five_field_job_and_an_interval_job_each_run_once_per_slot_across_two_skewed_runtimes()
 {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();

    let anchor = time::now() - ChronoDuration::hours(1);
    let mut one = runtime("se-cron-0");
    let mut two = runtime("se-cron-1");
    for (runtime, pod) in [(&mut one, "se-cron-0"), (&mut two, "se-cron-1")] {
        runtime
            .register(SampleCronJob::new(
                "minutely",
                Schedule::Cron(CronExpr::new("* * * * *").unwrap()),
                pod,
            ))
            .expect("a five-field job registers");
        runtime
            .register(SampleCronJob::new(
                "tick",
                Schedule::Every {
                    period: BEAT * 2,
                    anchor,
                },
                pod,
            ))
            .expect("an interval job registers");
    }

    for _ in 0..BEATS {
        one.beat(&pool).await;
        tokio::time::sleep(BEAT / 3).await;
        two.beat(&pool).await;
        tokio::time::sleep(BEAT * 2 / 3).await;
    }
    settle(&mut one, &mut two, &pool).await;

    for job in ["minutely", "tick"] {
        let slots = claimed_slots(&pool, job).await;
        assert!(slots >= 1, "{job} claimed no slot at all");
        assert_eq!(
            cron_runs(&pool, job).await,
            slots,
            "{job} must run exactly once per slot, however the two runtimes' beats interleave"
        );
        assert_eq!(
            completed_slots(&pool, job).await,
            slots,
            "{job} leaves every slot it ran marked complete, so no pod re-runs it"
        );
    }
    assert!(
        claimed_slots(&pool, "tick").await >= 2,
        "the interval job crossed several slots inside the run"
    );
    assert!(
        one.report().is_clean() && two.report().is_clean(),
        "a run that failed would be absorbed by the counters, so the board is checked first"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_slot_missed_while_no_runtime_was_up_runs_once_on_the_first_restart() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let missed = time::now() - ChronoDuration::minutes(90);

    let mut restarted = runtime("se-cron-0");
    restarted
        .register(SampleCronJob::new(
            "hourly",
            Schedule::Every {
                period: Duration::from_secs(3600),
                anchor: missed,
            },
            "se-cron-0",
        ))
        .expect("an hourly job registers");

    for _ in 0..4 {
        restarted.beat(&pool).await;
        tokio::time::sleep(BEAT).await;
    }
    settle_one(&mut restarted, &pool).await;

    assert_eq!(
        claimed_slots(&pool, "hourly").await,
        1,
        "the catch-up is bounded to the newest missed slot, not to every slot since the anchor"
    );
    assert_eq!(
        cron_runs(&pool, "hourly").await,
        1,
        "the first runtime that sees a missed slot runs it once, and never again"
    );

    let mut fresh = runtime("se-cron-1");
    fresh
        .register(SampleCronJob::new(
            "hourly",
            Schedule::Every {
                period: Duration::from_secs(3600),
                anchor: missed,
            },
            "se-cron-1",
        ))
        .expect("the second runtime registers the same job");
    for _ in 0..3 {
        fresh.beat(&pool).await;
        tokio::time::sleep(BEAT).await;
    }
    assert_eq!(
        cron_runs(&pool, "hourly").await,
        1,
        "a runtime that boots after the catch-up ran must not run the slot a second time"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_job_killed_mid_run_is_re_run_once_on_the_other_runtime_after_the_lease_expires() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s16_killed").await;

    let mut killed = runtime("se-cron-0");
    killed
        .register(
            SampleCronJob::new("stuck", Schedule::EveryBeats(600), "se-cron-0")
                .with_hold(Duration::from_secs(30)),
        )
        .expect("a job that outlives its pod registers");
    let round = killed.beat(&pool).await;
    assert_eq!(round.ran, 1, "the first runtime claims the slot and starts");
    assert_eq!(killed.running(), 1);
    drop(killed);

    let mut survivor = runtime("se-cron-1");
    survivor
        .register(SampleCronJob::new(
            "stuck",
            Schedule::EveryBeats(600),
            "se-cron-1",
        ))
        .expect("the surviving runtime registers the same job");

    let mut ran = false;
    for _ in 0..40 {
        survivor.beat(&pool).await;
        tokio::time::sleep(BEAT).await;
        if cron_runs(&pool, "stuck").await == 1 {
            ran = true;
            break;
        }
    }
    settle_one(&mut survivor, &pool).await;

    assert!(
        ran,
        "a slot whose holder died must be taken over once its lease expires, not lost"
    );
    assert_eq!(
        cron_pods(&pool, "stuck").await,
        vec!["se-cron-1".to_string()],
        "the run belongs to the runtime that took the slot over"
    );
    assert_eq!(claimed_slots(&pool, "stuck").await, 1);
    assert_eq!(cron_runs(&pool, "stuck").await, 1);

    pool.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn s16_a_job_that_outlives_a_beat_renews_its_lease_and_holds_no_transaction_open() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s16_long").await;

    let mut engine = runtime("se-cron-0");
    engine
        .register(
            SampleCronJob::new("long", Schedule::EveryBeats(600), "se-cron-0").with_hold(BEAT * 5),
        )
        .expect("a long job registers");

    engine.beat(&pool).await;
    let first = lease_until(&pool, "cron:long").await;
    let mut renewed = false;
    for _ in 0..30 {
        tokio::time::sleep(BEAT).await;
        engine.beat(&pool).await;
        assert_eq!(
            idle_in_transaction(db.app_pool()).await,
            0,
            "the run must hold no transaction open while it lasts"
        );
        if lease_until(&pool, "cron:long").await > first {
            renewed = true;
        }
        if cron_runs(&pool, "long").await == 1 {
            break;
        }
    }
    settle_one(&mut engine, &pool).await;

    assert!(
        renewed,
        "a run that outlives a beat renews its lease on each beat"
    );
    assert_eq!(cron_runs(&pool, "long").await, 1);
    assert_eq!(completed_slots(&pool, "long").await, 1);
    assert_eq!(engine.running(), 0);

    pool.close().await;
    db.cleanup().await;
}

async fn settle(one: &mut CronRuntime, two: &mut CronRuntime, pool: &PgPool) {
    settle_one(one, pool).await;
    settle_one(two, pool).await;
}

async fn settle_one(engine: &mut CronRuntime, pool: &PgPool) {
    for _ in 0..40 {
        if engine.running() == 0 {
            return;
        }
        tokio::time::sleep(BEAT).await;
        engine.beat(pool).await;
    }
    panic!("a run never settled");
}

async fn lease_until(pool: &PgPool, name: &str) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar("SELECT max(lease_until) FROM service_engine.leader_slot WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("read the lease bound")
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
