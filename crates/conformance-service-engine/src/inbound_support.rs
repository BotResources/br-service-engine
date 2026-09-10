use std::sync::Arc;
use std::time::Duration;

use async_nats::HeaderMap;
use br_core_integration::{Aggregate, Bc, CommandCoords, Verb};
use futures_util::future::BoxFuture;
use service_engine::inbound::{
    DeadLetters, Dispatch, EffectWriter, HEADER_PRODUCER, HEADER_SEQ, HEADER_SEQ_KEY,
    InboundConfig, InboundHealth, InboundLoop, Incoming, ReactionCoordinates, ReactionError,
    ReactionMessage, StubDispatch, StubPayload, Subscription,
};
use service_engine::nats::{Nats, command_subject};
use service_engine::pipeline::Reaction;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

pub fn sample_command_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("sample").unwrap(),
        aggregate: Aggregate::new("work").unwrap(),
        verb: Verb::new("do").unwrap(),
        version: 1,
    }
}

pub fn sample_subscription(durable: &str) -> Subscription {
    Subscription::new(
        durable,
        ReactionCoordinates::Command(sample_command_coords()),
    )
}

pub fn effect_writer() -> Arc<EffectWriter> {
    Arc::new(
        |conn: &mut PgConnection, msg: &Incoming| -> BoxFuture<'_, Result<(), sqlx::Error>> {
            let id = msg.message_id;
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO se_stub_effect (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
                )
                .bind(id)
                .execute(conn)
                .await
                .map(|_| ())
            })
        },
    )
}

pub fn stub_with_effect(pool: PgPool) -> StubDispatch {
    StubDispatch::new(pool).with_effect(effect_writer())
}

pub async fn start_loop(
    nats: &Nats,
    subscriptions: Vec<Subscription>,
    dispatch: Arc<dyn Dispatch>,
    pool: PgPool,
) -> InboundLoop {
    let (health, _health_rx) = InboundHealth::new();
    start_loop_with_health(nats, subscriptions, dispatch, pool, health).await
}

pub async fn start_loop_with_health(
    nats: &Nats,
    subscriptions: Vec<Subscription>,
    dispatch: Arc<dyn Dispatch>,
    pool: PgPool,
    health: InboundHealth,
) -> InboundLoop {
    InboundLoop::start(
        nats.clone(),
        subscriptions,
        dispatch,
        DeadLetters::new(pool),
        InboundConfig {
            ack_wait: Duration::from_secs(2),
            ..InboundConfig::default()
        },
        health,
    )
    .await
    .expect("the inbound loop binds its streams and starts")
}

pub async fn publish(
    nats: &Nats,
    message_id: Uuid,
    payload: &StubPayload,
    sequence: Option<(&str, &str, u64)>,
) {
    let subject = command_subject(&sample_command_coords());
    let mut headers = HeaderMap::new();
    headers.insert(async_nats::header::NATS_MESSAGE_ID, message_id.to_string());
    if let Some((producer, key, seq)) = sequence {
        headers.insert(HEADER_PRODUCER, producer);
        headers.insert(HEADER_SEQ_KEY, key);
        headers.insert(HEADER_SEQ, seq.to_string());
    }
    let bytes = serde_json::to_vec(payload).expect("the stub payload serializes");
    let ack = nats
        .context()
        .publish_with_headers(subject, headers, bytes.into())
        .await
        .expect("publish the command onto the integration command stream");
    ack.await.expect("the broker stores the published command");
}

pub async fn effect_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM se_stub_effect")
        .fetch_one(pool)
        .await
        .expect("count the stub effect rows")
}

pub async fn effect_present(pool: &PgPool, message_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM se_stub_effect WHERE message_id = $1")
        .bind(message_id)
        .fetch_one(pool)
        .await
        .expect("count the stub effect rows for one message")
}

pub async fn claim_rows(pool: &PgPool, message_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.message_claim WHERE message_id = $1")
        .bind(message_id)
        .fetch_one(pool)
        .await
        .expect("count the claim rows for one message")
}

pub async fn dead_letter_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter")
        .fetch_one(pool)
        .await
        .expect("count the dead-letter rows")
}

pub async fn wait_for_effect_rows(pool: &PgPool, expected: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let n = effect_rows(pool).await;
        if n >= expected || tokio::time::Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn wait_for_effect_present(pool: &PgPool, message_id: Uuid) -> i64 {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let n = effect_present(pool, message_id).await;
        if n >= 1 || tokio::time::Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn wait_for_dead_letter_rows(pool: &PgPool, expected: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let n = dead_letter_rows(pool).await;
        if n >= expected || tokio::time::Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn wait_for_sequence_guard(pool: &PgPool, producer: &str, seq_key: &str) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let last: Option<i64> = sqlx::query_scalar(
            "SELECT last_seq FROM service_engine.sequence_guard WHERE producer = $1 AND seq_key = $2",
        )
        .bind(producer)
        .bind(seq_key)
        .fetch_optional(pool)
        .await
        .expect("read the sequence guard");
        if last.is_some() || tokio::time::Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct SampleCommand {
    #[allow(dead_code)]
    pub verdict: String,
}

impl ReactionMessage for SampleCommand {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(sample_command_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

#[derive(Debug)]
pub struct SampleReactionError;

impl std::fmt::Display for SampleReactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sample reaction never runs in these scenarios")
    }
}

impl std::error::Error for SampleReactionError {}

impl ReactionError for SampleReactionError {
    fn disposition(&self) -> service_engine::inbound::Disposition {
        service_engine::inbound::Disposition::Retry
    }
}

pub fn sample_handler<'r>(
    _cx: &'r mut Reaction<'r>,
    _cmd: SampleCommand,
) -> BoxFuture<'r, Result<(), SampleReactionError>> {
    Box::pin(async { Ok(()) })
}
