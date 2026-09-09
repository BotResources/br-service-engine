use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, SampleDirectory, known_group_name, known_members, publish_group,
    publish_roster, retract_user_membership, staffing_mirror,
};
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s143_the_boot_join_merges_two_offers_and_retires_a_stale_group() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let stale = Uuid::now_v7();
    sqlx::query("INSERT INTO known_groups (group_id, name) VALUES ($1, $2)")
        .bind(stale)
        .bind("gone")
        .execute(&pool)
        .await
        .expect("a group mirrored by an earlier, wider run");

    let one = Uuid::now_v7();
    let two = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(one, "one@example.test"), (two, "two@example.test")]),
    )
    .await;
    let group = Uuid::now_v7();
    publish_group(&fabric, group, "team", &[one, two]).await;

    let mirror =
        staffing_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("the boot join reads both offers and projects the merged membership");

    assert_eq!(
        known_group_name(&pool, group).await.as_deref(),
        Some("team"),
        "replace_one projected the group row from the group offer"
    );
    assert_eq!(
        known_members(&pool, group).await,
        sorted(&[one, two]),
        "replace projected the members joined from the user offer"
    );
    assert_eq!(
        known_group_name(&pool, stale).await,
        None,
        "remove retired the group the bucket no longer offers"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s143_a_user_retract_reaches_every_group_the_user_was_joined_into() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let one = Uuid::now_v7();
    let two = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(one, "one@example.test"), (two, "two@example.test")]),
    )
    .await;
    let group = Uuid::now_v7();
    publish_group(&fabric, group, "team", &[one, two]).await;

    let mirror =
        staffing_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("the boot join projects the full membership");
    assert_eq!(known_members(&pool, group).await, sorted(&[one, two]));

    let watch = tokio::spawn(mirror.watch());
    retract_user_membership(&fabric, two).await;

    await_members(
        &pool,
        group,
        sorted(&[one]),
        "a user change never reached the group it was a member of through keyed_by",
    )
    .await;

    watch.abort();
    drop(nats);
    db.cleanup().await;
}

fn sorted(ids: &[Uuid]) -> Vec<Uuid> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids
}

async fn await_members(pool: &PgPool, group: Uuid, want: Vec<Uuid>, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if known_members(pool, group).await == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}
