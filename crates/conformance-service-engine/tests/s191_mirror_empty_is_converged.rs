//! Converged means bound, fully read once, and watched from the revision that
//! read reached. Content is not a readiness input: a prefix that reads empty is
//! a converged prefix with nothing in it, and `known_*` follows it to empty.
mod mirror_support;

use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use mirror_support::{
    OBSERVED_WITHIN, PUBLISHED_LANGUAGE, await_count, catalog, count, publish, retract, seed,
    watermark,
};
use service_engine::MirrorLeader;
use service_engine::error::EngineError;
use service_engine::name::PodId;
use sqlx::PgPool;
use uuid::Uuid;

const LEASE: Duration = Duration::from_secs(10);
const BEAT: Duration = Duration::from_millis(30);
const POLL: Duration = Duration::from_millis(20);

#[tokio::test]
async fn s191_an_empty_catalog_converges_at_boot_and_projects_known_star_to_empty() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    seed(&pool, "from an earlier run").await;

    catalog("empty_catalog")
        .build(
            nats.nats().await,
            pool.clone(),
            Arc::new(RecordingTransport),
        )
        .reconcile()
        .await
        .expect(
            "a bucket a producer has not published into yet is bound and fully read: that is \
             convergence, and the state of every producer's first deploy",
        );

    assert_eq!(
        count(&pool).await,
        0,
        "known_* follows the source; an empty source projects to empty, it does not freeze the \
         rows of an earlier run"
    );
    let held = watermark(&pool, "empty_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("an empty read is a read: it commits the boundary it reached");
    assert_eq!(held.0, 0);
    assert!(
        held.1.is_some(),
        "the boundary carries the stream identity it was read from"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s191_retracting_the_last_key_removes_a_row_keyed_only_by_the_retracted_payload() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let mirror = catalog("last_key_catalog").build(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
    );
    mirror.reconcile().await.expect("an empty boot converges");

    // The projection key is in the value, never in the KV key, so a retract
    // carries nothing the projector could key off after the value is gone.
    let id = Uuid::now_v7();
    let watching = tokio::spawn(mirror.watch());
    publish(&fabric, "catalog/arbitrary-source-key", id, "current").await;
    await_count(&pool, 1, "the watch projected the published catalog row").await;

    retract(&fabric, "catalog/arbitrary-source-key").await;
    await_count(
        &pool,
        0,
        "retracting the last key must remove the row it projected, even though only the \
         retracted payload carried its projection key",
    )
    .await;
    assert!(
        !watching.is_finished(),
        "a prefix that just went empty is still a converged prefix; the watch stays up"
    );

    watching.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s191_a_standby_on_an_empty_bucket_waits_for_the_leaders_committed_boundary() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();
    seed(&pool, "from an earlier run").await;

    sqlx::query(
        "INSERT INTO service_engine.leader_slot (name, slot, pod, lease_until, completed_at) \
         VALUES ('mirror:standby_catalog', 'epoch'::timestamptz, 'pod-a', \
                 now() + interval '1 hour', NULL)",
    )
    .execute(&pool)
    .await
    .expect("another pod holds the mirror's leader lease");

    let standby = catalog("standby_catalog").build_led(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
        MirrorLeader::new(PodId::new("pod-b").unwrap(), LEASE, BEAT),
    );
    let waiting = tokio::spawn(standby.reconcile());
    pending_while_uncommitted(&pool, &waiting).await;
    assert_eq!(
        count(&pool).await,
        1,
        "a standby projects nothing; an empty bucket does not make it erase the leader's rows"
    );

    catalog("standby_catalog")
        .build_led(
            fabric.clone(),
            pool.clone(),
            Arc::new(RecordingTransport),
            MirrorLeader::new(PodId::new("pod-a").unwrap(), LEASE, BEAT),
        )
        .reconcile()
        .await
        .expect("the leader projects the empty bucket and commits its boundary");

    tokio::time::timeout(OBSERVED_WITHIN, waiting)
        .await
        .expect("the standby converges once the leader has committed the boundary")
        .expect("the reconcile task did not panic")
        .expect("a caught-up standby reports converged");
    assert_eq!(
        count(&pool).await,
        0,
        "the leader's reconcile is what empties known_*, and the standby follows it"
    );

    drop(nats);
    db.cleanup().await;
}

/// The negative is tied to a condition rather than to a timer: for as long as
/// no leader has committed a boundary, a standby must not report converged.
async fn pending_while_uncommitted(
    pool: &PgPool,
    waiting: &tokio::task::JoinHandle<Result<(), EngineError>>,
) {
    for _ in 0..50 {
        let committed = watermark(pool, "standby_catalog", PUBLISHED_LANGUAGE)
            .await
            .is_some();
        assert!(
            committed || !waiting.is_finished(),
            "the standby reported converged while no leader had committed a boundary for it to \
             have caught up with"
        );
        if committed {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}
