use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, SampleDirectory, StagingTransport, directory_mirror, known_users,
    publish_roster, retract_user,
};
use service_engine::MirrorLeader;
use service_engine::name::PodId;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);
const LEASE: Duration = Duration::from_secs(1);
const BEAT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn s142_a_put_between_the_boot_read_and_the_watch_is_not_lost() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let first = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(first, "one@example.test")]),
    )
    .await;

    let mirror = directory_mirror().build(fabric.clone(), pool.clone(), StagingTransport::silent());
    mirror
        .reconcile()
        .await
        .expect("the boot full-read projects the bucket it saw");
    assert_eq!(known_users(&pool).await, 1);

    let gap = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(gap, "gap@example.test")]),
    )
    .await;

    let watch = tokio::spawn(mirror.watch());
    await_known(
        &pool,
        2,
        "the put that landed between the boot read and the watch was lost; the watch must resume \
         from the revision the read reached, not from now",
    )
    .await;

    watch.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s142_a_standby_is_not_ready_until_the_watermark_reaches_the_bucket_revision() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "one@example.test")]),
    )
    .await;

    sqlx::query(
        "INSERT INTO service_engine.leader_slot (name, slot, pod, lease_until, completed_at) \
         VALUES ('mirror:directory', 'epoch'::timestamptz, 'pod-a', now() + interval '1 hour', NULL)",
    )
    .execute(&pool)
    .await
    .expect("another pod holds the mirror's leader lease");

    let standby = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        StagingTransport::silent() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-b").unwrap(), LEASE, BEAT),
    );
    let converge = tokio::spawn(async move { standby.reconcile().await });

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !converge.is_finished(),
        "the standby loaded its shadows but the leader's watermark has not reached the bucket \
         revision, so it must not report converged yet"
    );
    assert_eq!(
        known_users(&pool).await,
        0,
        "a standby projects nothing; only the leader writes known_users"
    );

    sqlx::query(
        "INSERT INTO service_engine.mirror_watermark (mirror, bucket, revision) \
         VALUES ('directory', 'PUBLISHED_LANGUAGE', 9223372036854775807)",
    )
    .execute(&pool)
    .await
    .expect("the leader advances the watermark past the bucket revision");

    let outcome = tokio::time::timeout(OBSERVED_WITHIN, converge)
        .await
        .expect("the standby converges once the watermark has caught up")
        .expect("the reconcile task did not panic");
    outcome.expect("a caught-up standby reports converged");

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s142_emptying_a_consumed_prefix_at_run_time_takes_readiness_down_and_keeps_known_star() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect_err("a bucket that starts empty holds readiness down at boot");
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "only@example.test")]),
    )
    .await;
    mirror
        .reconcile()
        .await
        .expect("with one key the boot read converges");
    assert_eq!(known_users(&pool).await, 1);

    let watched = tokio::spawn(mirror.watch());
    tokio::time::sleep(Duration::from_millis(300)).await;
    retract_user(&fabric, user).await;

    let error = tokio::time::timeout(OBSERVED_WITHIN, watched)
        .await
        .expect("the watch stops when the last key of a consumed prefix is retracted")
        .expect("the watch task did not panic")
        .expect_err("emptying a consumed prefix during a run must take readiness down");
    let chain = error_chain(&error);
    assert!(
        chain.contains("identity/users/"),
        "the failure names the prefix that went empty, got {chain}"
    );
    assert_eq!(
        known_users(&pool).await,
        1,
        "a change that would empty a consumed prefix never erases known_*; the row is kept as is"
    );

    let back = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "only@example.test"), (back, "back@example.test")]),
    )
    .await;
    directory_mirror()
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("a refilled bucket converges again, so the pod goes back up");
    assert_eq!(known_users(&pool).await, 2);

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s142_the_periodic_reconcile_repairs_a_retract_the_watch_missed() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let kept = Uuid::now_v7();
    let stale = Uuid::now_v7();
    for (id, email) in [(kept, "kept@example.test"), (stale, "stale@example.test")] {
        sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .expect("a mirrored row from an earlier, wider run");
    }
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(kept, "kept@example.test")]),
    )
    .await;
    assert_eq!(known_users(&pool).await, 2);

    let mirror = directory_mirror()
        .with_reconcile_deadline(Duration::from_secs(1))
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    let watched = tokio::spawn(mirror.watch());

    await_known(
        &pool,
        1,
        "the periodic reconcile never re-read the bucket to retire the row its watch missed",
    )
    .await;
    let present: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
        .fetch_all(&pool)
        .await
        .expect("read the repaired roster");
    assert_eq!(present, vec![kept], "the periodic reconcile keeps only the offered user");

    watched.abort();
    drop(nats);
    db.cleanup().await;
}

async fn await_known(pool: &PgPool, want: i64, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if known_users(pool).await == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        message = format!("{message}: {next}");
        source = next.source();
    }
    message
}
