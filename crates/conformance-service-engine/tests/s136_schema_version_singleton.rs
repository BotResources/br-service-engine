use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine_config;
use service_engine::error::EngineError;
use service_engine::{Engine, Readiness, ReadinessHandle};

#[tokio::test]
async fn s136_a_second_pod_on_a_different_service_version_refuses_to_go_up() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let ready_a = ReadinessHandle::not_ready("boot");
    let engine_a = Engine::<SamplePrincipal>::boot(
        engine_config("se_schemaver_a", "pod-a").with_service_version("1.0.0"),
        db.app_pool().clone(),
        nats.nats().await,
        ready_a.clone(),
    )
    .await
    .expect("the first version boots and claims the schema version singleton");

    let ready_b = ReadinessHandle::not_ready("boot");
    let boot_b = Engine::<SamplePrincipal>::boot(
        engine_config("se_schemaver_b", "pod-b").with_service_version("2.0.0"),
        db.app_pool().clone(),
        nats.nats().await,
        ready_b.clone(),
    )
    .await;

    match boot_b {
        Err(EngineError::SchemaVersionConflict {
            live_service,
            service_version,
            ..
        }) => {
            assert_eq!(
                live_service, "1.0.0",
                "the conflict names the live pod's service version",
            );
            assert_eq!(
                service_version, "2.0.0",
                "the conflict names the refused pod's service version",
            );
        }
        other => panic!("a second, different service version must be refused, got {other:?}"),
    }

    match ready_b.snapshot() {
        Readiness::NotReady { reason } => {
            assert!(
                reason.contains("1.0.0") && reason.contains("2.0.0"),
                "the readiness reason names both service versions so the operator sees the \
                 mis-configured rolling deploy: {reason}",
            );
        }
        Readiness::Ready => panic!("a pod that found a different live version must stay DOWN"),
    }

    drop(engine_a);
    drop(nats);
    db.cleanup().await;
}
