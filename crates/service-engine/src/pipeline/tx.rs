use std::time::Duration;

use sqlx::PgConnection;

use crate::error::EngineError;
use crate::pipeline::staged::Staged;
use crate::transport::ImpactTransport;

async fn set_lock_timeout(
    conn: &mut PgConnection,
    lock_timeout: Duration,
) -> Result<(), sqlx::Error> {
    let millis = lock_timeout.as_millis().max(1);
    sqlx::query(&format!("SET LOCAL lock_timeout = {millis}"))
        .execute(conn)
        .await
        .map(|_| ())
}

pub(crate) async fn begin_scoped(
    pool: &sqlx::PgPool,
    lock_timeout: Duration,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Err(error) = set_lock_timeout(&mut tx, lock_timeout).await {
        let _ = tx.rollback().await;
        return Err(error);
    }
    Ok(tx)
}

pub(crate) async fn flush_and_commit(
    mut tx: sqlx::Transaction<'static, sqlx::Postgres>,
    staged: &Staged,
    transport: &dyn ImpactTransport,
) -> Result<(), EngineError> {
    if let Err(error) = staged.flush(&mut tx, transport).await {
        let _ = tx.rollback().await;
        return Err(error);
    }
    tx.commit().await.map_err(EngineError::from)?;
    crate::observe::record_impacts_committed(staged.impacts.len());
    Ok(())
}
