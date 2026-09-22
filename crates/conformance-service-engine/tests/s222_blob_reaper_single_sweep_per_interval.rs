#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::boot_blob_engine;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, ReaperRound};
use sqlx::PgPool;
use uuid::Uuid;

fn ran(rounds: &[ReaperRound]) -> usize {
    rounds.iter().filter(|round| round.ran).count()
}

fn skipped(rounds: &[ReaperRound]) -> usize {
    rounds.iter().filter(|round| round.skipped).count()
}

async fn reaper_slots(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.leader_slot WHERE name = 'reaper:blob'")
        .fetch_one(pool)
        .await
        .expect("count reaper slots")
}

// The leader gate is real, not a PK tautology: over N intervals with two pods up, exactly one
// pod claims the `reaper:blob` slot per interval and the other's sweep reports `skipped`. The
// load-bearing assertion is `ran_a + ran_b == distinct slot rows` — a pod that swept WITHOUT
// claiming a slot (Victor's bug) would record a `ran` round with no matching slot row, so the
// sum would exceed the row count. Removing the `claim_current_slot` guard from the engine (so
// every beat sweeps) would break this test two ways: no slot rows are inserted (slots == 0) and
// no round is ever `skipped`. Verified by reasoning; the mutation is not committed.
#[tokio::test]
async fn s222_two_pods_run_at_most_one_reaper_sweep_per_interval() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s222-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let interval = Duration::from_millis(300);

    let mut engine_a = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s222",
        "pod-s222a",
        minio.config(&bucket),
        policy,
        interval,
    )
    .await;
    let mut engine_b = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s222",
        "pod-s222b",
        minio.config(&bucket),
        policy,
        interval,
    )
    .await;
    let rounds_a = engine_a.observe_blob_reaper_rounds();
    let rounds_b = engine_b.observe_blob_reaper_rounds();
    let ready_a = engine_a.readiness();
    let ready_b = engine_b.readiness();
    let stop_a = engine_a.shutdown_handle();
    let stop_b = engine_b.shutdown_handle();
    let run_a = tokio::spawn(engine_a.run());
    let run_b = tokio::spawn(engine_b.run());
    await_ready(&ready_a).await;
    await_ready(&ready_b).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    stop_a.notify_one();
    stop_b.notify_one();
    run_a.await.expect("join a").expect("a returns Ok");
    run_b.await.expect("join b").expect("b returns Ok");

    let rounds_a = rounds_a.lock().unwrap().clone();
    let rounds_b = rounds_b.lock().unwrap().clone();
    let slots = reaper_slots(&pool).await;

    assert!(
        slots >= 2,
        "several reaper intervals elapsed while both pods were up (slots={slots})",
    );
    assert_eq!(
        (ran(&rounds_a) + ran(&rounds_b)) as i64,
        slots,
        "every reaper sweep that ran claimed exactly one slot; a pod sweeping without claiming a \
         slot would push the ran-count above the slot-row count (ran_a={}, ran_b={}, slots={slots})",
        ran(&rounds_a),
        ran(&rounds_b),
    );
    assert!(
        skipped(&rounds_a) + skipped(&rounds_b) >= 1,
        "the leader gate excluded at least one sweep attempt; without the slot claim no round \
         would ever be skipped (skipped_a={}, skipped_b={})",
        skipped(&rounds_a),
        skipped(&rounds_b),
    );

    let contended: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ( \
           SELECT slot FROM service_engine.leader_slot \
           WHERE name = 'reaper:blob' GROUP BY slot HAVING count(DISTINCT pod) > 1 \
         ) contended",
    )
    .fetch_one(&pool)
    .await
    .expect("count contended slots");
    assert_eq!(
        contended, 0,
        "no reaper:blob slot was ever held by two pods in the same interval",
    );

    drop(nats);
    drop(minio);
    db.cleanup().await;
}
