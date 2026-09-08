use async_nats::HeaderMap;
use br_core_integration::CommandCoords;
use serde::Serialize;
use service_engine::nats::{Nats, command_subject};
use sqlx::PgPool;
use uuid::Uuid;

pub async fn publish_command<T: Serialize>(
    nats: &Nats,
    coords: &CommandCoords,
    message_id: Uuid,
    payload: &T,
) {
    let subject = command_subject(coords);
    let mut headers = HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_ID,
        Uuid::now_v7().to_string(),
    );
    headers.insert(
        service_engine::inbound::HEADER_MESSAGE_ID,
        message_id.to_string(),
    );
    let bytes = serde_json::to_vec(payload).expect("the command payload serializes");
    let ack = nats
        .context()
        .publish_with_headers(subject, headers, bytes.into())
        .await
        .expect("publish the command onto the integration command stream");
    ack.await.expect("the broker stores the command");
}

pub async fn publish_raw(nats: &Nats, coords: &CommandCoords, message_id: Uuid, payload: &[u8]) {
    let subject = command_subject(coords);
    let mut headers = HeaderMap::new();
    headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id.to_string());
    let ack = nats
        .context()
        .publish_with_headers(subject, headers, payload.to_vec().into())
        .await
        .expect("publish the raw command onto the integration command stream");
    ack.await.expect("the broker stores the command");
}

pub async fn widget_count(pool: &PgPool, tenant: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_widget WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("count the widgets")
}

pub async fn widget_label(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT label FROM sample_widget WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read the widget label")
}

pub async fn insert_widget(pool: &PgPool, id: Uuid, tenant: Uuid, label: &str) {
    sqlx::query("INSERT INTO sample_widget (id, tenant_id, label) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(tenant)
        .bind(label)
        .execute(pool)
        .await
        .expect("insert the widget");
}

pub async fn wait_for_widget(pool: &PgPool, tenant: Uuid, expected: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let n = widget_count(pool, tenant).await;
        if n >= expected || tokio::time::Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
