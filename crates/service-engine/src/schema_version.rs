mod handover;

use std::time::Duration;

use sqlx::PgPool;

use crate::error::EngineError;

pub(crate) use handover::claim_with_handover;

const SCHEMA_VERSION_LOCK: i64 = 9_113_000_015;

pub(crate) async fn claim_schema_version(
    pool: &PgPool,
    engine_version: &str,
    service_version: &str,
    pod: &str,
    liveness: Duration,
) -> Result<(), EngineError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_VERSION_LOCK)
        .execute(&mut *tx)
        .await?;
    let live: Option<(String, String)> = sqlx::query_as(
        "SELECT engine_version, service_version FROM service_engine.schema_version \
         WHERE singleton \
           AND (engine_version <> $1 OR service_version <> $2) \
           AND heartbeat > now() - make_interval(secs => $3)",
    )
    .bind(engine_version)
    .bind(service_version)
    .bind(liveness.as_secs_f64())
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((live_engine, live_service)) = live {
        return Err(EngineError::SchemaVersionConflict {
            live_engine,
            live_service,
            engine_version: engine_version.to_string(),
            service_version: service_version.to_string(),
        });
    }
    sqlx::query(
        "INSERT INTO service_engine.schema_version \
           (singleton, engine_version, service_version, pod, heartbeat) \
         VALUES (true, $1, $2, $3, now()) \
         ON CONFLICT (singleton) DO UPDATE \
           SET engine_version = EXCLUDED.engine_version, \
               service_version = EXCLUDED.service_version, \
               pod = EXCLUDED.pod, \
               heartbeat = now()",
    )
    .bind(engine_version)
    .bind(service_version)
    .bind(pod)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn refresh_schema_version(
    pool: &PgPool,
    engine_version: &str,
    service_version: &str,
) -> Result<u64, EngineError> {
    let affected = sqlx::query(
        "UPDATE service_engine.schema_version SET heartbeat = now() \
         WHERE singleton AND engine_version = $1 AND service_version = $2",
    )
    .bind(engine_version)
    .bind(service_version)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(affected)
}

pub(crate) fn version_conflict_reason(
    live_engine: &str,
    live_service: &str,
    engine_version: &str,
    service_version: &str,
) -> String {
    format!(
        "another pod is live on service schema version {live_service} (engine {live_engine}) \
         and kept its heartbeat fresh through the whole handover wait; this pod is service \
         schema version {service_version} (engine {engine_version}); refusing to go UP so a \
         rolling deploy configured by mistake fails loud instead of two versions sharing the \
         store"
    )
}

pub(crate) fn handover_wait_reason(
    live_engine: &str,
    live_service: &str,
    engine_version: &str,
    service_version: &str,
    left: Duration,
) -> String {
    format!(
        "another pod is live on service schema version {live_service} (engine {live_engine}); \
         this pod is service schema version {service_version} (engine {engine_version}); \
         waiting up to {} ms for that heartbeat to lapse, as it does once the old pod of a \
         Recreate rollout has stopped, before refusing to go UP",
        left.as_millis()
    )
}
