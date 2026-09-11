use super::*;
use crate::name::{ChannelName, PodId};

fn config() -> EngineConfig {
    EngineConfig::new(
        ChannelName::from_static("service_engine_impact"),
        PodId::from_static("svc-sample-0"),
    )
}

#[test]
fn a_beat_takes_its_period_lease_and_retention_from_the_one_engine_config() {
    let beat = Beat::from_config(&config().with_beat(Duration::from_millis(250))).unwrap();
    assert_eq!(beat.interval(), Duration::from_millis(250));
    assert!(beat.readiness().is_none());
}

#[test]
fn a_config_that_would_expire_a_lease_before_its_holder_renews_it_never_starts_a_beat() {
    assert!(Beat::from_config(&config().with_lease(Duration::from_secs(1))).is_err());
}

#[test]
fn a_scheduled_batch_without_a_transport_to_claim_through_is_refused() {
    let beat = Beat::from_config(&config()).unwrap();
    assert!(beat.with_scheduled_batch(16).is_err());
}

#[test]
fn one_retention_governs_both_the_catch_up_horizon_and_the_slot_collector() {
    let mut beat = Beat::from_config(&config())
        .unwrap()
        .with_slot_retention(Duration::from_secs(3600));
    assert_eq!(beat.slot_retention(), Duration::from_secs(3600));
    beat.cron().set_slot_retention(Duration::from_secs(60));
    assert_eq!(
        beat.slot_retention(),
        Duration::from_secs(60),
        "the collector is handed the horizon the cron layer holds, so there is nothing to \
         desynchronise: collecting a slot row the cron layer would still claim makes the job \
         run twice"
    );
}

#[test]
fn a_round_that_left_work_behind_asks_the_beat_to_come_back_before_it_sleeps() {
    let round = BeatRound {
        relays: RelayRound {
            more: true,
            ..RelayRound::default()
        },
        ..BeatRound::default()
    };
    assert!(round.relays.more);
    assert!(!BeatRound::default().more);
}
