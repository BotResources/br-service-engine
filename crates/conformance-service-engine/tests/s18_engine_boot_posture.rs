#[allow(dead_code)]
mod engine_twin;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use service_engine::Engine;
use service_engine::boot::REASON_POSTURE_REFUSED;
use service_engine::error::EngineError;
use sqlx::PgPool;

const CHANNEL: &str = "se_s18_engine";

async fn refused(pool: PgPool, fabric: br_util_nats_fabric::Fabric, pod: &str) {
    let readiness = ReadinessHandle::ready();
    let outcome = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, pod),
        pool,
        fabric,
        readiness.clone(),
    )
    .await;
    match outcome {
        Ok(_) => panic!("Engine::boot accepted a role it must refuse ({pod})"),
        Err(EngineError::Posture(_)) => {}
        Err(other) => panic!("expected a posture refusal, got {other}"),
    }
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_POSTURE_REFUSED.to_string()
        },
        "a refused posture holds readiness DOWN with the posture reason"
    );
}

#[tokio::test]
async fn s18_engine_boot_refuses_every_over_privileged_role_and_accepts_the_app_role() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let ok = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, "pod-app"),
        db.app_pool().clone(),
        fabric.clone(),
        ReadinessHandle::ready(),
    )
    .await;
    assert!(
        ok.is_ok(),
        "the low-privilege runtime role is the sanctioned posture"
    );
    drop(ok);

    let superuser = db.superuser_pool().await;
    refused(superuser.clone(), fabric.clone(), "pod-super").await;
    superuser.close().await;

    let bypass = db
        .pool_as(db.bypass_role())
        .await
        .expect("a pool on the BYPASSRLS role");
    refused(bypass.clone(), fabric.clone(), "pod-bypass").await;
    bypass.close().await;

    let owner = db.owner_pool().clone();
    refused(owner, fabric.clone(), "pod-owner").await;

    drop(nats);
    db.cleanup().await;
}
