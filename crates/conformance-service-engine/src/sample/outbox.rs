use std::time::Duration;

use br_core_integration::{Aggregate, Bc, EventCoords, EventMetadata, IntegrationEvent, PastFact};
use br_core_kernel::{Actor, UserId};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use service_engine::nats::{INTEGRATION_EVT, Nats};
use service_engine::relays::outbox::{OutboxRecord, event_subject, stage as stage_outbox};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const QUIET_TAIL: Duration = Duration::from_millis(400);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relayed {
    pub label: String,
}

pub fn relayed_coords() -> EventCoords {
    EventCoords {
        producer: Bc::new("sample").unwrap(),
        aggregate: Aggregate::new("assignment").unwrap(),
        fact: PastFact::new("relayed").unwrap(),
        version: 1,
    }
}

pub fn envelope(event_id: Uuid, label: &str) -> IntegrationEvent<Relayed> {
    IntegrationEvent::new(
        event_id,
        "sample.assignment.relayed",
        1,
        Utc::now(),
        EventMetadata::new(Actor::Human(UserId::from(Uuid::now_v7())), Uuid::now_v7()),
        Relayed {
            label: label.to_string(),
        },
    )
}

pub async fn stage_outbox_row(conn: &mut PgConnection, label: &str) -> Uuid {
    let id = Uuid::now_v7();
    let record = OutboxRecord::stage_event(id, &relayed_coords(), &envelope(id, label))
        .expect("the envelope serializes into an outbox record");
    stage_outbox(&mut *conn, &record)
        .await
        .expect("the outbox row is staged on the caller's connection");
    id
}

pub async fn rewind_to_pending(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE service_engine.integration_outbox \
         SET status = 'PENDING', attempts = 0, last_error = NULL, published_at = NULL \
         WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("rewind the row to the state a crash between publish and mark leaves behind");
}

pub async fn row_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM service_engine.integration_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the outbox row status")
}

pub async fn delivered_event_ids(nats: &Nats, observer: &str) -> Vec<Uuid> {
    use futures_util::StreamExt;

    let subject = event_subject(&relayed_coords());
    let stream = nats
        .context()
        .get_stream(INTEGRATION_EVT)
        .await
        .expect("bind the fixed event stream");
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            name: Some(observer.to_string()),
            filter_subject: subject,
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("create an ephemeral observing consumer on the event stream");
    let mut messages = consumer
        .messages()
        .await
        .expect("open the message stream of the observing consumer");
    let mut ids = Vec::new();
    loop {
        match tokio::time::timeout(QUIET_TAIL, messages.next()).await {
            Ok(Some(Ok(message))) => {
                let event: IntegrationEvent<Relayed> = serde_json::from_slice(&message.payload)
                    .expect("a frame the relay published decodes");
                ids.push(event.event_id);
                message.ack().await.expect("ack the observed frame");
            }
            Ok(Some(Err(error))) => panic!("the observing consumer failed: {error}"),
            Ok(None) => break,
            Err(_elapsed) => break,
        }
    }
    let _ = stream.delete_consumer(observer).await;
    ids
}
