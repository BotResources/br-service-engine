use sqlx::PgConnection;

use crate::error::EngineError;
use crate::relays::outbox::store::OUTBOX_TABLE;

pub async fn adopt_legacy_outbox(conn: &mut PgConnection) -> Result<(), EngineError> {
    let legacy: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.integration_outbox')::text")
            .fetch_one(&mut *conn)
            .await
            .map_err(EngineError::Db)?;
    if legacy.is_none() {
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
    .execute(&mut *conn)
    .await
    .map_err(EngineError::Db)?;
    sqlx::query("DROP TABLE public.integration_outbox")
        .execute(&mut *conn)
        .await
        .map_err(EngineError::Db)?;
    Ok(())
}
