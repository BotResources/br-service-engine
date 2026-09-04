use futures_util::future::BoxFuture;
use futures_util::stream::{BoxStream, StreamExt};
use service_engine::error::{EngineError, TransportError};
use service_engine::impact::{Impact, TransportEvent};
use service_engine::name::NounName;
use service_engine::time::Timestamp;
use service_engine::transport::ImpactTransport;
use service_engine::wire::KeyBytes;
use sqlx::PgConnection;
use uuid::Uuid;

pub struct RecordingTransport;

impl ImpactTransport for RecordingTransport {
    fn stage_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            for impact in impacts {
                let payload =
                    serde_json::to_value(impact).map_err(|source| EngineError::Encode {
                        what: "impact",
                        source,
                    })?;
                sqlx::query("INSERT INTO sample_staged_impact (id, payload) VALUES ($1, $2)")
                    .bind(Uuid::now_v7())
                    .bind(payload)
                    .execute(&mut *conn)
                    .await?;
            }
            Ok(())
        })
    }

    fn schedule_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        noun: NounName,
        key: KeyBytes,
        at: Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO service_engine.scheduled_impact (id, at, noun, key) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(Uuid::now_v7())
            .bind(at)
            .bind(noun.as_str())
            .bind(
                serde_json::to_value(&key).map_err(|source| EngineError::Encode {
                    what: "scheduled key",
                    source,
                })?,
            )
            .execute(&mut *conn)
            .await?;
            Ok(())
        })
    }

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        futures_util::stream::empty().boxed()
    }
}

pub async fn staged_impacts(pool: &sqlx::PgPool) -> Vec<Impact> {
    let payloads: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM sample_staged_impact ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("read the impacts the recording transport staged");
    payloads
        .into_iter()
        .map(|payload| serde_json::from_value(payload).expect("a staged impact decodes"))
        .collect()
}
