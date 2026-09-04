use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use service_engine::accumulator::AccumulatorRuntime;
use service_engine::error::{EngineError, TransportError};
use service_engine::impact::{Impact, TransportEvent};
use service_engine::name::NounName;
use service_engine::time::Timestamp;
use service_engine::transport::ImpactTransport;
use service_engine::wire::KeyBytes;
use sqlx::{PgConnection, PgPool};

struct NoTransport;

impl ImpactTransport for NoTransport {
    fn stage_in<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn schedule_in<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _noun: NounName,
        _key: KeyBytes,
        _at: Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        Box::pin(stream::empty())
    }
}

pub fn idle_runtime(pg: PgPool) -> AccumulatorRuntime {
    AccumulatorRuntime::new(pg, Arc::new(NoTransport), Duration::from_secs(60))
}
