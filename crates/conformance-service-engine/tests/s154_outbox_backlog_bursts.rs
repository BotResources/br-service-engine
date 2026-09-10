use std::sync::Arc;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{delivered_event_ids, stage_outbox_row};
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::name::{PodId, RelayName};
use service_engine::relays::outbox::{HostedOutboxRelay, OutboxRelay};

const OUTBOX: RelayName = RelayName::from_static("integration_outbox");
const BACKLOG: usize = 1000;
const DRAIN_BOUND: usize = 256;

#[tokio::test]
async fn s154_a_thousand_row_backlog_drains_in_one_beat_burst() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..BACKLOG {
        stage_outbox_row(&mut tx, &format!("backlog-{n}")).await;
    }
    tx.commit().await.expect("commit the backlog");

    let relay = Arc::new(HostedOutboxRelay::hosting(
        OUTBOX,
        OutboxRelay::new(pool.clone(), fabric.clone()),
    ));
    let mut runtime = RelayRuntime::new(PodId::new("se-s154-0").expect("a valid pod id"));
    runtime
        .register_erased(relay.clone())
        .expect("the hosted relay registers");

    let mut passes = 0usize;
    let mut drained = 0usize;
    loop {
        let round = runtime.after_pass(&pool).await;
        assert_eq!(round.failed, 0, "no pass fails against a healthy broker");
        passes += 1;
        drained += round.rows;
        if !round.more {
            break;
        }
        assert!(
            passes < 16,
            "the burst must terminate long before the beat's ceiling"
        );
    }

    assert_eq!(
        drained, BACKLOG,
        "every backlogged row is drained within the one beat's burst"
    );
    let expected_passes = BACKLOG.div_ceil(DRAIN_BOUND);
    assert_eq!(
        passes, expected_passes,
        "with the cap matched to the drain bound each full pass signals more, so a 1000-row \
         backlog bursts across four passes in one beat instead of one pass per beat"
    );

    assert_eq!(
        delivered_event_ids(&fabric, "se-observer-s154").await.len(),
        BACKLOG,
        "every backlogged event reached the stream"
    );

    db.cleanup().await;
}
