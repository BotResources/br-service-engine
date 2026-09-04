use std::time::Duration;

use br_core_integration::{EventMetadata, IntegrationEvent};
use br_core_kernel::{Actor, UserId};
use br_util_nats_fabric::{
    Aggregate, Bc, EventCoords, Fabric, OutboxRecord, PastFact, stage as stage_outbox,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
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
    let record = OutboxRecord::stage_event(id, relayed_coords(), &envelope(id, label))
        .expect("the envelope serializes into an outbox record");
    stage_outbox(&mut *conn, &record)
        .await
        .expect("the outbox row is staged on the caller's connection");
    id
}

pub async fn rewind_to_pending(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE integration_outbox \
         SET status = 'PENDING', attempts = 0, last_error = NULL, published_at = NULL \
         WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("rewind the row to the state a crash between publish and mark leaves behind");
}

pub async fn row_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM integration_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the outbox row status")
}

pub async fn delivered_event_ids(fabric: &Fabric, durable: &str) -> Vec<Uuid> {
    let mut consumer = fabric
        .ensure_event_consumer::<Relayed>(&relayed_coords(), durable)
        .await
        .expect("bind a durable consumer on the fixed event stream");
    let mut ids = Vec::new();
    loop {
        match tokio::time::timeout(QUIET_TAIL, consumer.recv()).await {
            Ok(Ok(Some(delivery))) => {
                let id = delivery
                    .payload()
                    .expect("a frame the relay published decodes")
                    .event_id;
                ids.push(id);
                delivery.ack().await.expect("ack the observed frame");
            }
            Ok(Ok(None)) => break,
            Ok(Err(error)) => panic!("the durable consumer failed: {error}"),
            Err(_elapsed) => break,
        }
    }
    consumer.drain().await;
    ids
}
