use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::LeaderRunSampleRelay;
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::name::{PodId, RelayName};
use sqlx::PgPool;

const SLOT: Duration = Duration::from_millis(100);

#[tokio::test]
async fn s15_leader_runs_on_exactly_one_runtime_per_slot() {
    let db = TestDb::fresh().await;
    let one = db.pool_as(db.app_role()).await.expect("a pool per pod");
    let two = db.pool_as(db.app_role()).await.expect("a pool per pod");

    let mut first = runtime("se-leader-0");
    let mut second = runtime("se-leader-1");
    first
        .register(LeaderRunSampleRelay::new(RelayName::from_static("leader")))
        .expect("the leader relay registers");
    second
        .register(LeaderRunSampleRelay::new(RelayName::from_static("leader")))
        .expect("the leader relay registers");

    tokio::join!(
        beat_for(&mut first, &one, 24, Duration::from_millis(31)),
        beat_for(&mut second, &two, 24, Duration::from_millis(53)),
    );

    let slots: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM service_engine.leader_slot WHERE name = 'relay:leader'",
    )
    .await;
    let runs: i64 = scalar(db.app_pool(), "SELECT count(*) FROM sample_leader_run").await;
    let uncompleted: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM service_engine.leader_slot WHERE completed_at IS NULL",
    )
    .await;
    let doubled: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM (SELECT slot FROM sample_leader_run GROUP BY slot HAVING count(*) > 1) doubled",
    )
    .await;

    let pods: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT pod FROM sample_leader_run ORDER BY 1")
            .fetch_all(db.app_pool())
            .await
            .expect("read which pods ran a slot");

    assert!(
        first.health().borrow().is_healthy() && second.health().borrow().is_healthy(),
        "a drain that errored would be absorbed by the backoff, so the board must be clean \
         before any count below is trusted"
    );
    assert_eq!(pods.len(), 2, "both runtimes won at least one slot");
    assert!(slots >= 2, "the two beats crossed at least two slots");
    assert_eq!(runs, slots, "every claimed slot ran exactly once");
    assert_eq!(doubled, 0, "no slot was run by both runtimes");
    assert_eq!(uncompleted, 0, "a finished drain marks its slot completed");

    let strays: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM service_engine.leader_slot \
          WHERE slot <> date_bin(interval '100 milliseconds', slot, 'epoch'::timestamptz) \
             OR slot > now()",
    )
    .await;
    assert_eq!(
        strays, 0,
        "every slot is the database clock quantised, never a pod's: a skewed pod would write \
         an unquantised or future slot and invent a slot of its own"
    );

    one.close().await;
    two.close().await;
    db.cleanup().await;
}

fn runtime(pod: &str) -> RelayRuntime {
    RelayRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_batch(8)
        .with_slot_period(SLOT)
        .with_lease(Duration::from_secs(30))
}

async fn beat_for(engine: &mut RelayRuntime, pg: &PgPool, rounds: usize, gap: Duration) {
    for _ in 0..rounds {
        engine.beat(pg).await;
        tokio::time::sleep(gap).await;
    }
}

async fn scalar(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
