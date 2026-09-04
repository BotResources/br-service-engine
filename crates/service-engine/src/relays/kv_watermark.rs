use br_util_nats_fabric::KvKey;
use sqlx::{PgConnection, Row};

use crate::error::RelayError;
use crate::name::RelayName;

pub(crate) async fn read(
    conn: &mut PgConnection,
    relay: &RelayName,
    key: &KvKey,
) -> Result<Option<u64>, RelayError> {
    let row = sqlx::query(
        "SELECT version FROM service_engine.kv_relay_watermark WHERE relay = $1 AND kv_key = $2",
    )
    .bind(relay.as_str())
    .bind(key.as_str())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.and_then(|row| u64::try_from(row.get::<i64, _>("version")).ok()))
}

pub(crate) async fn raise(
    conn: &mut PgConnection,
    relay: &RelayName,
    key: &KvKey,
    version: u64,
) -> Result<(), RelayError> {
    let stored = i64::try_from(version).map_err(|_| {
        RelayError::Publish(format!(
            "published-language version {version} for {} exceeds the storable watermark range",
            key.as_str()
        ))
    })?;
    sqlx::query(
        "INSERT INTO service_engine.kv_relay_watermark (relay, kv_key, version) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (relay, kv_key) DO UPDATE \
         SET version = GREATEST(kv_relay_watermark.version, EXCLUDED.version)",
    )
    .bind(relay.as_str())
    .bind(key.as_str())
    .bind(stored)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
