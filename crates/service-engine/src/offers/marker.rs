use bytes::Bytes;
use sqlx::{PgConnection, Row};

use crate::error::RelayError;
use crate::name::RelayName;
use crate::nats::KvKey;
use crate::schema::TABLE_OFFER_DIRTY;
use crate::wire::KeyBytes;

pub(crate) struct Marker {
    pub(crate) kv_key: KvKey,
    pub(crate) agg_key: Option<KeyBytes>,
    pub(crate) seq: u64,
}

pub(crate) async fn pending(
    conn: &mut PgConnection,
    name: &RelayName,
    batch: usize,
) -> Result<Vec<Marker>, RelayError> {
    let rows = sqlx::query(&format!(
        "SELECT kv_key, agg_key, seq FROM {TABLE_OFFER_DIRTY} \
         WHERE offer = $1 ORDER BY seq LIMIT $2 \
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(name.as_str())
    .bind(i64::try_from(batch).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter().map(row_to_marker).collect()
}

fn row_to_marker(row: sqlx::postgres::PgRow) -> Result<Marker, RelayError> {
    let kv_key = KvKey::new(row.get::<String, _>("kv_key")).map_err(|error| {
        RelayError::Publish(format!("offer kv key: {}", crate::chain::describe(&error)))
    })?;
    let agg_key = row
        .get::<Option<Vec<u8>>, _>("agg_key")
        .map(|bytes| KeyBytes::from_json_bytes(Bytes::from(bytes)))
        .transpose()
        .map_err(relay)?;
    let seq = u64::try_from(row.get::<i64, _>("seq")).unwrap_or_default();
    Ok(Marker {
        kv_key,
        agg_key,
        seq,
    })
}

pub(crate) async fn stage(
    conn: &mut PgConnection,
    name: &RelayName,
    kv_key: &KvKey,
    agg_key: Option<&KeyBytes>,
) -> Result<(), RelayError> {
    sqlx::query(&format!(
        "INSERT INTO {TABLE_OFFER_DIRTY} (offer, kv_key, agg_key, seq) \
         VALUES ($1, $2, $3, nextval('service_engine.offer_dirty_seq')) \
         ON CONFLICT (offer, kv_key) DO UPDATE \
           SET agg_key = EXCLUDED.agg_key, seq = nextval('service_engine.offer_dirty_seq')"
    ))
    .bind(name.as_str())
    .bind(kv_key.as_str())
    .bind(agg_key.map(|key| key.as_slice().to_vec()))
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn delete(
    conn: &mut PgConnection,
    name: &RelayName,
    marker: &Marker,
) -> Result<(), RelayError> {
    sqlx::query(&format!(
        "DELETE FROM {TABLE_OFFER_DIRTY} WHERE offer = $1 AND kv_key = $2 AND seq = $3"
    ))
    .bind(name.as_str())
    .bind(marker.kv_key.as_str())
    .bind(i64::try_from(marker.seq).unwrap_or(i64::MAX))
    .execute(conn)
    .await?;
    Ok(())
}

fn relay(error: crate::error::EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}
