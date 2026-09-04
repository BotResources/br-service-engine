use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::{engine_config, pool_named};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::principal::SamplePrincipal;
use service_engine::Engine;
use service_engine::boot::REASON_MIRRORS;
use service_engine::error::EngineError;
use service_engine::transport::ListenerProbe;
use service_engine::transport::probe::POOLER_REASON;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s26_pooler_probe() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let deaf_readiness = ReadinessHandle::ready();
    let refused = Engine::<SamplePrincipal>::boot_with_probe(
        engine_config("se_s26_deaf_impact", "pod-a").with_listener_probe_timeout(PROBE_TIMEOUT),
        pool_named(&db, db.app_role(), "se_s26_deaf").await,
        fabric.clone(),
        deaf_readiness.clone(),
        ListenerProbe::deaf(),
    )
    .await
    .expect_err("a listener that cannot hear its own probe never becomes ready");
    assert!(
        matches!(
            refused,
            EngineError::ProbeTimeout { probe: "listener", timeout } if timeout == PROBE_TIMEOUT
        ),
        "expected a typed probe timeout, got {refused}"
    );
    match deaf_readiness.snapshot() {
        Readiness::NotReady { reason } => assert_eq!(
            reason, POOLER_REASON,
            "the engine, not the test, set readiness DOWN with the pooler reason on the Err path"
        ),
        Readiness::Ready => panic!("a service that cannot LISTEN must never report ready"),
    }

    let healthy_readiness = ReadinessHandle::ready();
    let engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s26_healthy_impact", "pod-b").with_listener_probe_timeout(PROBE_TIMEOUT),
        pool_named(&db, db.app_role(), "se_s26_healthy").await,
        fabric,
        healthy_readiness.clone(),
    )
    .await
    .expect("a listener that hears its own probe passes the boot gate");
    match healthy_readiness.snapshot() {
        Readiness::NotReady { reason } => assert_eq!(
            reason, REASON_MIRRORS,
            "a listener that passed its probe no longer holds readiness down for the listener"
        ),
        Readiness::Ready => panic!("readiness is the engine's to raise, not the transport's"),
    }
    drop(engine);

    drop(nats);
    db.cleanup().await;
}
