use std::time::Duration;

use crate::blobs::BlobConfig;
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
    assert_eq!(DURATIONS.len(), 17);
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

fn with_blob(blob: BlobConfig) -> EngineConfig {
    let mut candidate = config();
    candidate.blob = Some(blob);
    candidate
}

fn blob() -> BlobConfig {
    BlobConfig::new("https://s3.internal", "eu", "blobs", "AKIA", "secret")
}

#[test]
fn a_blob_presign_ttl_the_signer_would_turn_inert_is_refused_at_boot_by_name() {
    let download = [
        Duration::ZERO,
        Duration::from_millis(999),
        Duration::from_secs(7 * 24 * 60 * 60 + 1),
    ];
    for ttl in download {
        match with_blob(blob().with_download_ttl(ttl)).validate() {
            Err(EngineError::Config(message)) => assert!(message.contains("download_ttl")),
            other => panic!("download_ttl {ttl:?} must be refused at boot, got {other:?}"),
        }
    }
    match with_blob(blob().with_upload_ttl(Duration::from_micros(999))).validate() {
        Err(EngineError::Config(message)) => assert!(message.contains("upload_ttl")),
        other => panic!("a sub-millisecond upload_ttl must be refused at boot, got {other:?}"),
    }
}

#[test]
fn short_blob_ttls_the_scenarios_rely_on_still_boot() {
    with_blob(
        blob()
            .with_upload_ttl(Duration::from_millis(300))
            .with_download_ttl(Duration::from_secs(1)),
    )
    .validate()
    .expect("a few hundred milliseconds of upload TTL is valid");
}

#[test]
fn a_blob_storage_with_blank_credentials_is_still_left_to_the_bind() {
    with_blob(BlobConfig::new("", "", "", "", ""))
        .validate()
        .expect("an unused blank blob config is accepted at boot and refused at bind");
}

#[test]
fn every_engine_config_field_is_classified_so_a_new_one_cannot_skip_validation() {
    let EngineConfig {
        window,
        beat,
        session_ttl,
        session_buffer,
        reset_threshold,
        max_held_impacts,
        seal_retention,
        max_buffered_chunks,
        fold_cache_capacity,
        lease,
        offer_reconcile,
        mirror_reconcile,
        listener_probe_timeout,
        repair_attempts,
        session_max_age,
        lock_timeout,
        ack_wait,
        max_ack_pending,
        listener_queue_threshold: _,
        listener_channel_capacity,
        nats_grace,
        window_capacity,
        impacts_per_commit,
        message_retention,
        publish_ack_timeout,
        blob_reaper_interval,
        schema_version_liveness,
        http_addr: _,
        migrate_connect_timeout: _,
        channel: _,
        pod_id: _,
        nats_url: _,
        app_role: _,
        service: _,
        service_version: _,
        blob: _,
        max_body_bytes,
        multipart: _,
        body_read_timeout,
    } = config();
    let durations: [(&str, Duration); 17] = [
        ("window", window),
        ("beat", beat),
        ("session_ttl", session_ttl),
        ("seal_retention", seal_retention),
        ("lease", lease),
        ("offer_reconcile", offer_reconcile),
        ("mirror_reconcile", mirror_reconcile),
        ("listener_probe_timeout", listener_probe_timeout),
        ("session_max_age", session_max_age),
        ("lock_timeout", lock_timeout),
        ("ack_wait", ack_wait),
        ("nats_grace", nats_grace),
        ("message_retention", message_retention),
        ("publish_ack_timeout", publish_ack_timeout),
        ("body_read_timeout", body_read_timeout),
        ("blob_reaper_interval", blob_reaper_interval),
        ("schema_version_liveness", schema_version_liveness),
    ];
    let counts: [(&str, u64); 11] = [
        ("session_buffer", session_buffer as u64),
        ("max_buffered_chunks", max_buffered_chunks as u64),
        ("fold_cache_capacity", fold_cache_capacity as u64),
        ("reset_threshold", reset_threshold as u64),
        ("max_held_impacts", max_held_impacts as u64),
        ("window_capacity", window_capacity as u64),
        ("impacts_per_commit", impacts_per_commit as u64),
        (
            "listener_channel_capacity",
            listener_channel_capacity as u64,
        ),
        ("repair_attempts", u64::from(repair_attempts)),
        ("max_ack_pending", max_ack_pending as u64),
        ("max_body_bytes", max_body_bytes),
    ];
    let duration_labels: Vec<&str> = durations.iter().map(|(label, _)| *label).collect();
    let matrix_labels: Vec<&str> = DURATIONS.iter().map(|(label, _)| *label).collect();
    assert_eq!(duration_labels, matrix_labels);
    let count_labels: Vec<&str> = counts.iter().map(|(label, _)| *label).collect();
    let zero_labels: Vec<&str> = ZERO_BOUNDS.iter().map(|(label, _)| *label).collect();
    assert_eq!(count_labels, zero_labels);
}
