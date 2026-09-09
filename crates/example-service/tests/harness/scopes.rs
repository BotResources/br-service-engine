use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use br_core_integration::{Actor, EventMetadata, IntegrationEvent, ServiceAccountId};
use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use service_engine::nats::Nats;
use tokio::task::JoinHandle;
use uuid::Uuid;

const DECLARE_SUBJECT: &str = "integration.cmd.identity.service_scope.declare.v1";
const ACCEPTED_SUBJECT: &str = "integration.evt.identity.service_scope.accepted.v1";

#[derive(Deserialize)]
struct DeclareProbe {
    metadata: EventMetadata,
}

pub struct ScopeIdentity {
    seen: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl ScopeIdentity {
    pub fn declarations_seen(&self) -> usize {
        self.seen.load(Ordering::SeqCst)
    }
}

impl Drop for ScopeIdentity {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn accept_scopes(nats: &Nats) -> ScopeIdentity {
    let client = nats.context().client().clone();
    let mut declarations = client
        .subscribe(DECLARE_SUBJECT)
        .await
        .expect("the stand-in identity subscribes to the declaration command subject");
    let seen = Arc::new(AtomicUsize::new(0));
    let publisher = client.clone();
    let counter = seen.clone();
    let task = tokio::spawn(async move {
        while let Some(message) = declarations.next().await {
            let Ok(probe) = serde_json::from_slice::<DeclareProbe>(&message.payload) else {
                continue;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let event = IntegrationEvent::new(
                Uuid::now_v7(),
                "service_scope.accepted",
                1,
                Utc::now(),
                EventMetadata::new(
                    Actor::Service(ServiceAccountId::from(Uuid::now_v7())),
                    probe.metadata.correlation_id,
                ),
                serde_json::json!({ "service": "example" }),
            );
            let bytes = serde_json::to_vec(&event).expect("the accepted event encodes");
            let _ = publisher.publish(ACCEPTED_SUBJECT, bytes.into()).await;
        }
    });
    ScopeIdentity { seen, task }
}
