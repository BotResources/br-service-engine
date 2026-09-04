use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::metrics_probe;
use conformance_service_engine::sample::LeaderRunSampleRelay;
use service_engine::Beat;
use service_engine::EngineConfig;
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::metrics::{LABEL_OUTCOME, LEADER_SLOT_CLAIMS_TOTAL};
use service_engine::name::{ChannelName, PodId, RelayName};

const ONE_SLOT: Duration = Duration::from_secs(3600);

fn runtime(pod: &str) -> RelayRuntime {
    RelayRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_batch(8)
        .with_slot_period(ONE_SLOT)
        .with_lease(Duration::from_secs(30))
}

#[tokio::test]
async fn s15_a_leader_relay_beat_reports_the_slot_it_won_and_the_slot_it_lost() {
    let db = TestDb::fresh().await;
    let winner_pool = db.pool_as(db.app_role()).await.expect("a pool per pod");
    let loser_pool = db.pool_as(db.app_role()).await.expect("a pool per pod");

    let mut winner = runtime("se-relay-win");
    let mut loser = runtime("se-relay-lose");
    winner
        .register(LeaderRunSampleRelay::new(RelayName::from_static("leader")))
        .expect("the leader relay registers");
    loser
        .register(LeaderRunSampleRelay::new(RelayName::from_static("leader")))
        .expect("the leader relay registers");

    let won = winner.beat(&winner_pool).await;
    assert_eq!(
        won.slot_won, 1,
        "the runtime that claimed the leader slot reports it as a slot won, not a bare drain"
    );
    assert_eq!(won.slot_skipped, 0);

    let lost = loser.beat(&loser_pool).await;
    assert_eq!(
        lost.slot_skipped, 1,
        "a runtime that lost the same slot to another pod reports a skipped leader claim, never \
         conflated with an idle row-claim relay"
    );
    assert_eq!(lost.slot_won, 0);

    winner_pool.close().await;
    loser_pool.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn s15_the_leader_slot_metric_counts_a_relay_win_not_only_a_cron_win() {
    let probe = metrics_probe::install();
    let before = probe.labelled_total(LEADER_SLOT_CLAIMS_TOTAL, LABEL_OUTCOME, "won");

    let db = TestDb::fresh().await;
    let pool = db
        .pool_as(db.app_role())
        .await
        .expect("a pool for the beat");

    let config = EngineConfig::new(
        ChannelName::from_static("se_s15_relay_metric"),
        PodId::from_static("se-relay-metric"),
    )
    .with_beat(ONE_SLOT)
    .with_lease(Duration::from_secs(7200));
    let mut beat = Beat::from_config(&config).expect("the beat assembles");
    beat.relays()
        .register(LeaderRunSampleRelay::new(RelayName::from_static("leader")))
        .expect("the leader relay registers on the beat");

    let round = beat.tick(&pool).await;
    assert_eq!(
        round.relays.slot_won, 1,
        "the single beat won the leader slot for the relay"
    );

    let after = probe.labelled_total(LEADER_SLOT_CLAIMS_TOTAL, LABEL_OUTCOME, "won");
    assert!(
        after > before,
        "the leader-slot-claims metric rose for a relay win, so its name no longer lies by \
         counting cron wins alone (before={before}, after={after})"
    );

    pool.close().await;
    db.cleanup().await;
}
