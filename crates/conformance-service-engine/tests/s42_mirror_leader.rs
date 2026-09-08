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

const LEASE: Duration = Duration::from_secs(1);
const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s42_exactly_one_pod_projects_and_the_standby_takes_over_when_the_leader_stops() {
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

    let transport_a = StagingTransport::silent();
    let transport_b = StagingTransport::silent();
    let mirror_a = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        transport_a.clone() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-a").unwrap(), LEASE),
    );
    let mirror_b = directory_mirror().build_led(
        fabric.clone(),
        pool.clone(),
        transport_b.clone() as Arc<dyn ImpactTransport>,
        MirrorLeader::new(PodId::new("pod-b").unwrap(), LEASE),
    );

    mirror_a
        .reconcile()
        .await
        .expect("pod A loads its shadows and, as the first to take the lease, projects");
    mirror_b
        .reconcile()
        .await
        .expect("pod B loads its shadows but, as the standby, projects nothing");

    assert_eq!(known_users(&pool).await, 1, "the roster is projected once");
    assert!(
        !transport_a.staged().is_empty(),
        "pod A holds the lease, so it projected and staged the impact"
    );
    assert!(
        transport_b.staged().is_empty(),
        "pod B is the standby, so it kept its shadow current without projecting"
    );

    let watch_a = tokio::spawn(mirror_a.watch());
    let watch_b = tokio::spawn(mirror_b.watch());

    let second = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(first, "one@example.test"), (second, "two@example.test")]),
    )
    .await;
    await_known(&pool, 2, "the live leader never projected the second user").await;
    let b_after_second = transport_b.staged().len();
    assert_eq!(
        b_after_second, 0,
        "while pod A is alive and holds the lease, pod B still projects nothing"
    );

    watch_a.abort();
    tokio::time::sleep(LEASE + Duration::from_millis(750)).await;

    let third = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[
            (first, "one@example.test"),
            (second, "two@example.test"),
            (third, "three@example.test"),
        ]),
    )
    .await;
    await_known(
        &pool,
        3,
        "the standby never took over after the leader stopped",
    )
    .await;
    assert!(
        !transport_b.staged().is_empty(),
        "pod B took the expired lease and projected the change the stopped leader would have"
    );

    watch_b.abort();
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
