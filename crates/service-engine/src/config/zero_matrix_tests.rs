use std::time::Duration;

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::name::{ChannelName, PodId};

fn config() -> EngineConfig {
    EngineConfig::new(
        ChannelName::new("service_engine_impact").unwrap(),
        PodId::new("svc-sample-0").unwrap(),
    )
}

type Zero = fn(&mut EngineConfig);
type SetDuration = fn(&mut EngineConfig, Duration);

const DURATIONS: &[(&str, SetDuration)] = &[
    ("window", |c, value| c.window = value),
    ("beat", |c, value| c.beat = value),
    ("session_ttl", |c, value| c.session_ttl = value),
    ("seal_retention", |c, value| c.seal_retention = value),
    ("lease", |c, value| c.lease = value),
    ("offer_reconcile", |c, value| c.offer_reconcile = value),
    ("mirror_reconcile", |c, value| c.mirror_reconcile = value),
    ("listener_probe_timeout", |c, value| {
        c.listener_probe_timeout = value
    }),
    ("session_max_age", |c, value| c.session_max_age = value),
    ("lock_timeout", |c, value| c.lock_timeout = value),
    ("ack_wait", |c, value| c.ack_wait = value),
    ("nats_grace", |c, value| c.nats_grace = value),
    ("message_retention", |c, value| c.message_retention = value),
    ("publish_ack_timeout", |c, value| {
        c.publish_ack_timeout = value
    }),
    ("body_read_timeout", |c, value| c.body_read_timeout = value),
    ("blob_reaper_interval", |c, value| {
        c.blob_reaper_interval = value
    }),
    ("schema_version_liveness", |c, value| {
        c.schema_version_liveness = value
    }),
    ("migrate_connect_timeout", |c, value| {
        c.migrate_connect_timeout = value
    }),
];

const ZERO_BOUNDS: &[(&str, Zero)] = &[
    ("session_buffer", |c| c.session_buffer = 0),
    ("max_buffered_chunks", |c| c.max_buffered_chunks = 0),
    ("fold_cache_capacity", |c| c.fold_cache_capacity = 0),
    ("reset_threshold", |c| c.reset_threshold = 0),
    ("max_held_impacts", |c| c.max_held_impacts = 0),
    ("window_capacity", |c| c.window_capacity = 0),
    ("impacts_per_commit", |c| c.impacts_per_commit = 0),
    ("listener_channel_capacity", |c| {
        c.listener_channel_capacity = 0
    }),
    ("repair_attempts", |c| c.repair_attempts = 0),
    ("max_ack_pending", |c| c.max_ack_pending = 0),
    ("max_body_bytes", |c| c.max_body_bytes = 0),
];

fn refusal_naming(label: &str, zero: impl FnOnce(&mut EngineConfig)) -> String {
    let mut candidate = config();
    zero(&mut candidate);
    match candidate.validate() {
        Err(EngineError::Config(message)) => message,
        other => panic!(
            "{label} below its floor must be refused as a configuration error, got {other:?}"
        ),
    }
}

#[test]
fn every_duration_the_engine_waits_on_is_refused_below_one_microsecond_by_name() {
    assert_eq!(DURATIONS.len(), 18);
    for (label, set) in DURATIONS {
        for value in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_nanos(999),
        ] {
            let message = refusal_naming(label, |c| set(c, value));
            assert!(
                message.contains(label),
                "the refusal of {label} at {value:?} must name it, got: {message}"
            );
        }
    }
}

#[test]
fn exactly_one_microsecond_is_the_smallest_accepted_blob_reaper_interval() {
    let mut candidate = config();
    candidate.blob_reaper_interval = Duration::from_micros(1);
    candidate
        .validate()
        .expect("one microsecond is the finest slot a Postgres interval can hold");
}

#[test]
fn every_count_or_capacity_the_engine_sizes_from_is_refused_at_zero_by_name() {
    for (label, zero) in ZERO_BOUNDS {
        let message = refusal_naming(label, *zero);
        assert!(
            message.contains(label),
            "the refusal of a zero {label} must name it, got: {message}"
        );
    }
}

#[test]
fn a_duration_left_at_its_default_is_never_refused() {
    config().validate().expect("the defaults validate");
}
