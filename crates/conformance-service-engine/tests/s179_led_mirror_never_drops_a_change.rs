use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    SampleDirectory, StagingTransport, directory_mirror, known_users, publish_roster,
};
use service_engine::MirrorLeader;
use service_engine::name::PodId;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;
use uuid::Uuid;

const LEASE: Duration = Duration::from_secs(5);
const BEAT: Duration = Duration::from_millis(200);
const HELD_FOR: Duration = Duration::from_millis(400);
const POLL: Duration = Duration::from_millis(50);
// Far below the default periodic reconcile (300 s): a change that arrives here was
// projected by the live watch, never rescued by a rescan.
const PROJECTED_WITHIN: Duration = Duration::from_secs(5);
// `lock_id(LEADER_SLOT, ["mirror", "directory"])`: the advisory lock a led
// `directory` mirror takes around each projection.
const DIRECTORY_MIRROR_LOCK: i64 = 2_050_405_624_873_511_262;
const LOAD: usize = 40;

#[tokio::test]
async fn s179_a_change_that_meets_a_busy_advisory_lock_is_projected_once_the_lock_frees() {
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
    let mirror = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        StagingTransport::silent() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-a").unwrap(), LEASE, BEAT),
    );
    mirror
        .reconcile()
        .await
        .expect("the only pod takes the lease and projects the roster");
    assert_eq!(known_users(&pool).await, 1);
    let watch = tokio::spawn(mirror.watch());

    let mut holder = db
        .owner_pool()
        .acquire()
        .await
        .expect("a second session to hold the mirror's advisory lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(DIRECTORY_MIRROR_LOCK)
        .execute(&mut *holder)
        .await
        .expect("hold the lock the way a standby's projection attempt does");

    let second = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(second, "two@example.test")]),
    )
    .await;
    tokio::time::sleep(HELD_FOR).await;
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(DIRECTORY_MIRROR_LOCK)
        .execute(&mut *holder)
        .await
        .expect("release the lock");

    let third = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(third, "three@example.test")]),
    )
    .await;

    await_known(
        &pool,
        3,
        "the change published while the advisory lock was busy was dropped by the leader: it \
         would only come back at the periodic reconcile",
    )
    .await;

    watch.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s179_two_pods_under_load_project_every_change_from_the_live_watch() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let seed = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(seed, "seed@example.test")]),
    )
    .await;
    let mirror_a = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        StagingTransport::silent() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-a").unwrap(), LEASE, BEAT),
    );
    let mirror_b = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        StagingTransport::silent() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-b").unwrap(), LEASE, BEAT),
    );
    mirror_a.reconcile().await.expect("pod A leads");
    mirror_b.reconcile().await.expect("pod B stands by");
    let watch_a = tokio::spawn(mirror_a.watch());
    let watch_b = tokio::spawn(mirror_b.watch());

    for n in 0..LOAD {
        let email = format!("user{n}@example.test");
        publish_roster(
            &fabric,
            &SampleDirectory::with_users(&[(Uuid::now_v7(), email.as_str())]),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    await_known(
        &pool,
        1 + LOAD as i64,
        "with a standby watching the same bucket, the leader dropped changes under load",
    )
    .await;

    watch_a.abort();
    watch_b.abort();
    drop(nats);
    db.cleanup().await;
}

async fn await_known(pool: &PgPool, want: i64, note: &str) {
    let deadline = tokio::time::Instant::now() + PROJECTED_WITHIN;
    loop {
        let held = known_users(pool).await;
        if held == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{note} (projected {held} of {want})"
        );
        tokio::time::sleep(POLL).await;
    }
}
