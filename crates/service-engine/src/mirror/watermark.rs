use sqlx::PgConnection;

use crate::error::EngineError;
use crate::schema::TABLE_MIRROR_WATERMARK;

pub(super) async fn read(
    conn: &mut PgConnection,
    mirror: &str,
    bucket: &str,
) -> Result<u64, EngineError> {
    let revision: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT revision FROM {TABLE_MIRROR_WATERMARK} WHERE mirror = $1 AND bucket = $2"
    ))
    .bind(mirror)
    .bind(bucket)
    .fetch_optional(conn)
    .await?;
    Ok(revision.unwrap_or(0).max(0) as u64)
}

pub(super) async fn advance(
    conn: &mut PgConnection,
    mirror: &str,
    bucket: &str,
    revision: u64,
) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "INSERT INTO {TABLE_MIRROR_WATERMARK} (mirror, bucket, revision) VALUES ($1, $2, $3) \
         ON CONFLICT (mirror, bucket) DO UPDATE \
           SET revision = GREATEST({TABLE_MIRROR_WATERMARK}.revision, EXCLUDED.revision)"
    ))
    .bind(mirror)
    .bind(bucket)
    .bind(i64::try_from(revision).unwrap_or(i64::MAX))
    .execute(conn)
    .await?;
    Ok(())
}
