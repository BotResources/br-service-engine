mod mirror_support;

use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use mirror_support::{OBSERVED_WITHIN, PUBLISHED_LANGUAGE, catalog, email_of, publish, watermark};
use service_engine::MirrorLeader;
use service_engine::name::PodId;
use service_engine::nats::Nats;
use uuid::Uuid;

const HELD_LEASE: Duration = Duration::from_secs(60);
const SHORT_LEASE: Duration = Duration::from_secs(1);
const BEAT: Duration = Duration::from_millis(200);
const WAITED_OUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s195_a_standby_converges_against_a_leader_watermark_that_carries_no_identity() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let id = Uuid::now_v7();
    publish(&fabric, "catalog/one", id, "one").await;

    catalog("upgrading_catalog")
        .build_led(
            fabric.clone(),
            pool.clone(),
            Arc::new(RecordingTransport),
            MirrorLeader::new(PodId::new("pod-old").unwrap(), HELD_LEASE, BEAT),
        )
        .reconcile()
        .await
        .expect("the pod that takes the lease projects and commits the boundary");
    sqlx::query(
        "UPDATE service_engine.mirror_watermark SET stream_created_at = NULL \
         WHERE mirror = 'upgrading_catalog'",
    )
    .execute(&pool)
    .await
    .expect("the lease holder is a 0.1.0 leader: a boundary, and no identity");

    let standby = catalog("upgrading_catalog").build_led(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
        MirrorLeader::new(PodId::new("pod-new").unwrap(), HELD_LEASE, BEAT),
    );
    tokio::time::timeout(OBSERVED_WITHIN, standby.reconcile())
        .await
        .expect("a standby must not wait out the upgrade for an identity the old leader can never write")
        .expect("it converges on the boundary the row does carry");

    assert_eq!(
        watermark(&pool, "upgrading_catalog", PUBLISHED_LANGUAGE)
            .await
            .expect("the row is still the leader's")
            .1,
        None,
        "the standby converged without writing: the identity is adopted when this pod takes the \
         lease, not while it stands by"
    );
    assert_eq!(email_of(&pool, id).await.as_deref(), Some("one"));

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s195_a_standby_whose_leader_holds_another_identity_adopts_rather_than_failing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let id = Uuid::now_v7();
    publish(&fabric, "catalog/one", id, "one").await;

    catalog("displaced_catalog")
        .build_led(
            fabric.clone(),
            pool.clone(),
            Arc::new(RecordingTransport),
            MirrorLeader::new(PodId::new("pod-a").unwrap(), HELD_LEASE, BEAT),
        )
        .reconcile()
        .await
        .expect("pod A takes the lease and commits the identity it read");
    sqlx::query(
        "UPDATE service_engine.mirror_watermark \
         SET stream_created_at = 'a stream this pod never read' \
         WHERE mirror = 'displaced_catalog'",
    )
    .execute(&pool)
    .await
    .expect("the leader converged on another stream identity");

    let standby = catalog("displaced_catalog").build_led(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
        MirrorLeader::new(PodId::new("pod-b").unwrap(), SHORT_LEASE, BEAT),
    );
    let converging = tokio::spawn(standby.reconcile());
    tokio::time::sleep(WAITED_OUT).await;
    assert!(
        !converging.is_finished(),
        "an identity the standby did not read is a wait, not a failure: finishing here means it \
         returned an error the supervisor would count as a restart"
    );

    sqlx::query(
        "UPDATE service_engine.leader_slot SET lease_until = now() - make_interval(secs => 1) \
         WHERE name = 'mirror:displaced_catalog'",
    )
    .execute(&pool)
    .await
    .expect("the leader stops renewing");
    tokio::time::timeout(OBSERVED_WITHIN, converging)
        .await
        .expect("the standby takes the expired lease")
        .expect("the reconcile task lives")
        .expect("a replaced identity is an adoption, never a configuration error");

    let held = watermark(&pool, "displaced_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("the boundary is still committed");
    assert_eq!(
        held.1.as_deref(),
        Some(last_created(&fabric).await.as_str()),
        "the standby took the expired lease and adopted the identity of the stream it read"
    );
    assert_eq!(email_of(&pool, id).await.as_deref(), Some("one"));

    drop(nats);
    db.cleanup().await;
}

async fn last_created(fabric: &Nats) -> String {
    fabric
        .bind_stream(&format!("KV_{PUBLISHED_LANGUAGE}"))
        .await
        .expect("the consumed bucket's stream")
        .get_info()
        .await
        .expect("fresh stream metadata")
        .created
        .to_string()
}
