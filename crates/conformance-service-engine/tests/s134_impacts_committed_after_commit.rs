use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::{engine_config, pool_named};
use conformance_service_engine::metrics_probe;
use service_engine::impact::{Dims, Impact};
use service_engine::metrics::IMPACTS_COMMITTED_TOTAL;
use service_engine::name::NounName;
use service_engine::time;
use service_engine::transport::{ImpactTransport, PgListenNotify};
use service_engine::wire::KeyBytes;
use uuid::Uuid;

const CHANNEL: &str = "se_s134_impact";
const LISTENER: &str = "se_s134_listener";
const SCHEDULED: usize = 3;

fn impacts(count: usize) -> Vec<Impact> {
    (0..count)
        .map(|_| Impact::ResourceChanged {
            noun: NounName::from_static("assignment"),
            key: KeyBytes::encode(&Uuid::now_v7()).expect("a key encodes"),
            dims: Dims::EMPTY,
            cause: None,
        })
        .collect()
}

#[tokio::test]
async fn s134_impacts_committed_counts_committed_impacts_only() {
    let probe = metrics_probe::install();
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), LISTENER).await;
    let transport = PgListenNotify::connect(pool.clone(), &engine_config(CHANNEL, "pod-a"))
        .await
        .expect("the listener is established");

    let baseline = probe.total_by_name(IMPACTS_COMMITTED_TOTAL);

    let rolled_back = impacts(5);
    let mut tx = pool.begin().await.expect("open a write transaction");
    transport
        .stage_in(&mut tx, &rolled_back)
        .await
        .expect("stage impacts that will never commit");
    tx.rollback().await.expect("roll the staging back");

    assert_eq!(
        probe.total_by_name(IMPACTS_COMMITTED_TOTAL),
        baseline,
        "staging impacts into a transaction that rolls back records nothing on the \
         notify-budget counter"
    );

    let mut tx = pool.begin().await.expect("open the scheduling transaction");
    for _ in 0..SCHEDULED {
        transport
            .schedule_in(
                &mut tx,
                NounName::from_static("assignment"),
                KeyBytes::encode(&Uuid::now_v7()).expect("a key encodes"),
                time::now(),
            )
            .await
            .expect("schedule a boundary whose time has already passed");
    }
    tx.commit().await.expect("commit the schedule rows");

    let before = probe.total_by_name(IMPACTS_COMMITTED_TOTAL);
    let fired = transport
        .fire_due(SCHEDULED as i64)
        .await
        .expect("the beat fires the due boundaries in one committed transaction");
    assert_eq!(fired, SCHEDULED, "every due row fired");
    let after = probe.total_by_name(IMPACTS_COMMITTED_TOTAL);

    assert_eq!(
        after,
        before + SCHEDULED as u64,
        "a committed transaction records exactly its impacts on the notify-budget counter; the \
         rolled-back staging before it contributed nothing (before={before}, after={after})"
    );

    db.cleanup().await;
}
