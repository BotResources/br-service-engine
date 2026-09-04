use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use service_engine::config::EngineConfig;
use service_engine::error::TransportError;
use service_engine::impact::TransportEvent;
use service_engine::name::{ChannelName, PodId};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::infra::TestDb;

pub const BACKEND_SETTLE: Duration = Duration::from_millis(50);
pub const BACKEND_GONE_TIMEOUT: Duration = Duration::from_secs(10);

pub type EventStream = BoxStream<'static, Result<TransportEvent, TransportError>>;

pub fn engine_config(channel: &str, pod: &str) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new(channel).expect("a valid notify channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
}

pub async fn pool_named(db: &TestDb, role: &str, application_name: &str) -> PgPool {
    let url = format!("{}?application_name={application_name}", db.url_as(role));
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap_or_else(|e| {
            panic!("connect as {role} with application_name {application_name}: {e}")
        })
}

pub async fn next_event(
    stream: &mut EventStream,
    within: Duration,
) -> Option<Result<TransportEvent, TransportError>> {
    tokio::time::timeout(within, stream.next())
        .await
        .unwrap_or_default()
}

pub async fn expect_event(stream: &mut EventStream, within: Duration) -> TransportEvent {
    next_event(stream, within)
        .await
        .expect("the transport delivered an event before the deadline")
        .expect("the transport event is not a fatal error")
}

pub async fn backends_named(db: &TestDb, application_name: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name = $2",
    )
    .bind(db.database())
    .bind(application_name)
    .fetch_one(db.admin_pool())
    .await
    .expect("read pg_stat_activity")
}

pub async fn terminate_and_wait(db: &TestDb, application_name: &str) {
    db.terminate_backends(application_name).await;
    let deadline = tokio::time::Instant::now() + BACKEND_GONE_TIMEOUT;
    loop {
        if backends_named(db, application_name).await == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "backends named {application_name} outlived pg_terminate_backend"
        );
        tokio::time::sleep(BACKEND_SETTLE).await;
    }
}
