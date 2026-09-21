use sqlx::{Connection, PgConnection};

use crate::error::EngineError;
use crate::relays::outbox::store::OUTBOX_TABLE;

const ADOPT_LOCK_KEY: i64 = 0x6f75_7462_6f78;

pub async fn adopt_legacy_outbox(conn: &mut PgConnection) -> Result<(), EngineError> {
    let mut tx = conn.begin().await.map_err(EngineError::Db)?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(ADOPT_LOCK_KEY)
        .execute(&mut *tx)
        .await
        .map_err(EngineError::Db)?;
    let legacy: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.integration_outbox')::text")
            .fetch_one(&mut *tx)
            .await
            .map_err(EngineError::Db)?;
    if legacy.is_none() {
        tx.commit().await.map_err(EngineError::Db)?;
        return Ok(());
    }
    sqlx::query(&format!(
        "INSERT INTO {OUTBOX_TABLE} \
           (id, subject, payload, status, attempts, producer, seq_key, seq, last_error, \
            published_at, created_at) \
         SELECT id, subject, payload, status, attempts, producer, seq_key, seq, last_error, \
                published_at, created_at \
         FROM public.integration_outbox \
         ON CONFLICT (id) DO NOTHING"
    ))
    .execute(&mut *tx)
    .await
    .map_err(EngineError::Db)?;
    sqlx::query("DROP TABLE IF EXISTS public.integration_outbox")
        .execute(&mut *tx)
        .await
        .map_err(EngineError::Db)?;
    tx.commit().await.map_err(EngineError::Db)?;
    Ok(())
}
