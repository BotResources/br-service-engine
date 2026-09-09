use std::time::Duration;

use br_core_integration::{Actor, EventMetadata, IntegrationEvent, UserId};
use br_core_scope::{
    ScopeDeclarationError, ServiceKey, ServiceScopesAccepted, ServiceScopesRejected,
};
use chrono::Utc;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_render_engine;
use futures_util::StreamExt;
use serde::Deserialize;
use service_engine::ScopeManifest;
use service_engine::{Readiness, ReadinessHandle};
use tokio::task::JoinHandle;
use uuid::Uuid;

const DECLARE_SUBJECT: &str = "integration.cmd.identity.service_scope.declare.v1";
const ACCEPTED_SUBJECT: &str = "integration.evt.identity.service_scope.accepted.v1";
const REJECTED_SUBJECT: &str = "integration.evt.identity.service_scope.rejected.v1";

const READY_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

#[derive(Deserialize)]
struct CorrelationProbe {
    metadata: EventMetadata,
}

enum IdentityReply {
    Accept,
    Reject,
}

async fn spawn_identity(
    url: String,
    service: &'static str,
    reply: IdentityReply,
) -> JoinHandle<()> {
    let client = async_nats::connect(&url)
        .await
        .expect("the fake identity responder dials the broker");
    let js = async_nats::jetstream::new(client.clone());
    let mut declares = client
        .subscribe(DECLARE_SUBJECT)
        .await
        .expect("subscribe to the declare command before the engine publishes it");
    tokio::spawn(async move {
        while let Some(message) = declares.next().await {
            let Ok(probe) = serde_json::from_slice::<CorrelationProbe>(&message.payload) else {
                continue;
            };
            let correlation_id = probe.metadata.correlation_id;
            let service_key = ServiceKey::new(service).expect("a valid fake service key");
            let (subject, payload) = match reply {
                IdentityReply::Accept => (
                    ACCEPTED_SUBJECT,
                    serde_json::to_vec(&IntegrationEvent::new(
                        Uuid::now_v7(),
                        "service_scope.accepted",
                        1,
                        Utc::now(),
                        reply_metadata(correlation_id),
                        ServiceScopesAccepted::new(service_key),
                    ))
                    .expect("encode the accepted confirmation"),
                ),
                IdentityReply::Reject => (
                    REJECTED_SUBJECT,
                    serde_json::to_vec(&IntegrationEvent::new(
                        Uuid::now_v7(),
                        "service_scope.rejected",
                        1,
                        Utc::now(),
                        reply_metadata(correlation_id),
                        ServiceScopesRejected::new(
                            service_key,
                            ScopeDeclarationError::ScopePrefixMismatch {
                                scope_service: "billing".to_string(),
                                declaring_service: service.to_string(),
                            },
                        ),
                    ))
                    .expect("encode the rejected confirmation"),
                ),
            };
            js.publish(subject.to_string(), payload.into())
                .await
                .expect("publish the confirmation onto INTEGRATION_EVT")
                .await
                .expect("the confirmation is stored");
        }
    })
}

fn reply_metadata(correlation_id: Uuid) -> EventMetadata {
    EventMetadata::new(Actor::Human(UserId::from(Uuid::nil())), correlation_id)
}

#[tokio::test]
async fn an_accepted_declaration_brings_the_pod_up() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let identity = spawn_identity(nats.url(), "project", IdentityReply::Accept).await;

    let mut engine =
        boot_render_engine(&db, nats.nats().await, "se_scopes_accept", "pod-accept").await;
    engine
        .declare_scopes(ScopeManifest::of(&[&["project:read", "project:archive"]]))
        .expect("a single-service manifest is declared before boot");
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok on a clean shutdown once identity accepted");
    identity.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn a_rejected_declaration_keeps_the_pod_down_with_the_reason() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let identity = spawn_identity(nats.url(), "project", IdentityReply::Reject).await;

    let mut engine =
        boot_render_engine(&db, nats.nats().await, "se_scopes_reject", "pod-reject").await;
    engine
        .declare_scopes(ScopeManifest::of(&[&["project:read"]]))
        .expect("the manifest is well-formed; identity is what refuses it");
    let readiness = engine.readiness();

    let error = tokio::spawn(engine.run())
        .await
        .expect("the engine task joins")
        .expect_err("run returns loudly when identity rejects the declaration");
    assert!(
        matches!(error, service_engine::EngineError::Scope(_)),
        "a rejection surfaces as a scope error, not a generic failure: {error:?}"
    );

    match readiness.snapshot() {
        Readiness::NotReady { reason } => assert!(
            reason.contains("scope declaration rejected"),
            "the readiness payload carries the rejection reason, got: {reason}"
        ),
        Readiness::Ready => panic!("a rejected pod must never read UP"),
    }
    identity.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn a_service_that_declares_nothing_becomes_ready_without_a_handshake() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let watcher = async_nats::connect(&nats.url())
        .await
        .expect("a watcher dials the broker to prove no declaration is ever sent");
    let mut declares = watcher
        .subscribe(DECLARE_SUBJECT)
        .await
        .expect("subscribe to the declare subject");

    let engine = boot_render_engine(&db, nats.nats().await, "se_scopes_none", "pod-none").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let heard = tokio::time::timeout(Duration::from_millis(300), declares.next()).await;
    assert!(
        heard.is_err(),
        "a scopeless engine skips the gate entirely and never publishes a scope declaration"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("a scopeless engine shuts down cleanly");
    drop(nats);
    db.cleanup().await;
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP: the scope gate or a boot condition never composed green"
        );
        tokio::time::sleep(POLL).await;
    }
}
