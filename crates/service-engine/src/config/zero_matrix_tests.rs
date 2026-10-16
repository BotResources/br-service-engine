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

const ZERO_DURATIONS: &[(&str, Zero)] = &[
    ("window", |c| c.window = Duration::ZERO),
    ("beat", |c| c.beat = Duration::ZERO),
    ("session_ttl", |c| c.session_ttl = Duration::ZERO),
    ("seal_retention", |c| c.seal_retention = Duration::ZERO),
    ("lease", |c| c.lease = Duration::ZERO),
    ("offer_reconcile", |c| c.offer_reconcile = Duration::ZERO),
    ("mirror_reconcile", |c| c.mirror_reconcile = Duration::ZERO),
    ("listener_probe_timeout", |c| {
        c.listener_probe_timeout = Duration::ZERO
    }),
    ("session_max_age", |c| c.session_max_age = Duration::ZERO),
    ("lock_timeout", |c| c.lock_timeout = Duration::ZERO),
    ("ack_wait", |c| c.ack_wait = Duration::ZERO),
    ("nats_grace", |c| c.nats_grace = Duration::ZERO),
    ("message_retention", |c| {
        c.message_retention = Duration::ZERO
    }),
    ("publish_ack_timeout", |c| {
        c.publish_ack_timeout = Duration::ZERO
    }),
    ("body_read_timeout", |c| {
        c.body_read_timeout = Duration::ZERO
    }),
    ("blob_reaper_interval", |c| {
        c.blob_reaper_interval = Duration::ZERO
    }),
    ("schema_version_liveness", |c| {
        c.schema_version_liveness = Duration::ZERO
    }),
    ("migrate_connect_timeout", |c| {
        c.migrate_connect_timeout = Duration::ZERO
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

fn refusal_naming(label: &str, zero: Zero) -> String {
    let mut candidate = config();
    zero(&mut candidate);
    match candidate.validate() {
        Err(EngineError::Config(message)) => message,
        other => panic!("a zero {label} must be refused as a configuration error, got {other:?}"),
    }
}

#[test]
fn every_duration_the_engine_waits_on_is_refused_at_zero_by_name() {
    for (label, zero) in ZERO_DURATIONS {
        let message = refusal_naming(label, *zero);
        assert!(
            message.contains(label),
            "the refusal of a zero {label} must name it, got: {message}"
        );
    }
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
