use crate::config::EngineConfig;
use crate::name::{ChannelName, PodId};
use std::time::Duration;

fn config() -> EngineConfig {
    EngineConfig::new(
        ChannelName::new("service_engine_impact").unwrap(),
        PodId::new("svc-sample-0").unwrap(),
    )
}

#[test]
fn a_fresh_config_carries_the_documented_defaults() {
    let c = config();
    assert_eq!(c.window, Duration::from_millis(100));
    assert_eq!(c.beat, Duration::from_secs(1));
    assert_eq!(c.session_ttl, Duration::from_secs(30));
    assert_eq!(c.session_buffer, 256);
    assert_eq!(c.reset_threshold, 200);
    assert_eq!(c.max_held_impacts, 1_024);
    assert_eq!(c.seal_retention, Duration::from_secs(86_400));
    assert_eq!(c.max_buffered_chunks, 10_000);
    assert_eq!(c.fold_cache_capacity, 10_000);
    assert_eq!(c.lease, Duration::from_secs(30));
    assert_eq!(c.offer_reconcile, Duration::from_secs(300));
    assert_eq!(c.mirror_reconcile, Duration::from_secs(300));
    assert_eq!(c.listener_probe_timeout, Duration::from_secs(2));
    assert_eq!(c.repair_attempts, 5);
    assert_eq!(c.session_max_age, Duration::from_secs(43_200));
    assert_eq!(c.lock_timeout, Duration::from_secs(5));
    assert_eq!(c.ack_wait, Duration::from_secs(30));
    assert_eq!(c.max_ack_pending, 256);
    assert_eq!(c.listener_queue_threshold, 0.5);
    assert_eq!(c.listener_channel_capacity, 1_024);
    assert_eq!(c.nats_grace, Duration::from_secs(10));
    assert_eq!(c.window_capacity, 10_000);
    assert_eq!(c.impacts_per_commit, 1_000);
    assert_eq!(c.publish_ack_timeout, Duration::from_secs(2));
    assert_eq!(c.body_read_timeout, Duration::from_secs(30));
    assert_eq!(c.service, None);
    c.validate().unwrap();
}

#[test]
fn a_publish_ack_timeout_at_or_above_ack_wait_or_zero_is_refused() {
    for bad in [Duration::from_secs(30), Duration::ZERO] {
        assert!(config().with_publish_ack_timeout(bad).validate().is_err());
    }
    config()
        .with_publish_ack_timeout(Duration::from_secs(29))
        .validate()
        .expect("a publish ack timeout below ack_wait is accepted");
}

#[test]
fn a_zero_duration_or_zero_bound_is_refused() {
    assert!(config().with_window(Duration::ZERO).validate().is_err());
    assert!(config().with_session_buffer(0).validate().is_err());
    assert!(config().with_reset_threshold(0).validate().is_err());
    assert!(config().with_max_held_impacts(0).validate().is_err());
    assert!(config().with_max_buffered_chunks(0).validate().is_err());
    assert!(config().with_fold_cache_capacity(0).validate().is_err());
    assert!(config().with_repair_attempts(0).validate().is_err());
    assert!(
        config()
            .with_lock_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(config().with_nats_grace(Duration::ZERO).validate().is_err());
    assert!(
        config()
            .with_body_read_timeout(Duration::ZERO)
            .validate()
            .is_err(),
        "a zero body read timeout would refuse every authenticated body"
    );
    assert!(
        config()
            .with_offer_reconcile(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        config()
            .with_mirror_reconcile(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(config().with_window_capacity(0).validate().is_err());
    assert!(config().with_impacts_per_commit(0).validate().is_err());
    assert!(
        config()
            .with_listener_channel_capacity(0)
            .validate()
            .is_err()
    );
}

#[test]
fn a_session_max_age_that_is_not_above_the_idle_ttl_is_refused() {
    assert!(
        config()
            .with_session_max_age(Duration::from_secs(30))
            .validate()
            .is_err()
    );
    assert!(
        config()
            .with_session_ttl(Duration::from_secs(30))
            .with_session_max_age(Duration::from_secs(31))
            .validate()
            .is_ok()
    );
}

#[test]
fn a_listener_queue_threshold_outside_the_open_unit_interval_is_refused() {
    assert!(
        config()
            .with_listener_queue_threshold(0.0)
            .validate()
            .is_err()
    );
    assert!(
        config()
            .with_listener_queue_threshold(1.0)
            .validate()
            .is_ok()
    );
    assert!(
        config()
            .with_listener_queue_threshold(1.5)
            .validate()
            .is_err()
    );
    assert!(
        config()
            .with_listener_queue_threshold(f64::NAN)
            .validate()
            .is_err()
    );
}

#[test]
fn a_lock_timeout_that_is_not_below_ack_wait_is_refused() {
    assert!(
        config()
            .with_ack_wait(Duration::from_secs(5))
            .with_lock_timeout(Duration::from_secs(5))
            .validate()
            .is_err(),
        "a lock_timeout equal to ack_wait lets a transaction still hold its lock at redelivery"
    );
    assert!(
        config()
            .with_ack_wait(Duration::from_secs(30))
            .with_lock_timeout(Duration::from_secs(31))
            .validate()
            .is_err()
    );
    assert!(
        config()
            .with_ack_wait(Duration::from_secs(30))
            .with_lock_timeout(Duration::from_secs(5))
            .validate()
            .is_ok()
    );
}

#[test]
fn a_non_positive_max_ack_pending_is_refused() {
    assert!(config().with_max_ack_pending(0).validate().is_err());
    assert!(config().with_max_ack_pending(-1).validate().is_err());
    assert!(config().with_max_ack_pending(1).validate().is_ok());
}

#[test]
fn a_zero_ack_wait_is_refused() {
    assert!(config().with_ack_wait(Duration::ZERO).validate().is_err());
}

#[test]
fn a_lease_that_does_not_outlast_the_beat_is_refused() {
    assert!(
        config()
            .with_lease(Duration::from_secs(1))
            .validate()
            .is_err()
    );
}

#[test]
fn a_schema_version_liveness_that_does_not_outlast_the_beat_is_refused() {
    let beat = Duration::from_millis(500);
    for liveness in [Duration::ZERO, beat - Duration::from_millis(1), beat] {
        let refused = config()
            .with_beat(beat)
            .with_schema_version_liveness(liveness);
        assert!(refused.validate().is_err(), "liveness {liveness:?}");
    }
    let accepted = config()
        .with_beat(beat)
        .with_schema_version_liveness(beat * 2);
    assert!(accepted.validate().is_ok());
}

#[test]
fn a_blank_service_label_is_refused_but_an_absent_one_is_accepted() {
    assert!(config().with_service("  ").validate().is_err());
    assert!(config().with_service("svc-chat").validate().is_ok());
    assert!(config().validate().is_ok());
}
