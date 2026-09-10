use std::time::Duration;

use sqlx::{PgConnection, PgPool, Row};

use crate::accumulator::guard;
use crate::accumulator::{ChunkSeq, Registered};
use crate::error::EngineError;
use crate::time::Timestamp;
use crate::wire::KeyBytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealMarker {
    high_water: ChunkSeq,
    sealed_at: Timestamp,
}

impl SealMarker {
    pub fn high_water(&self) -> ChunkSeq {
        self.high_water
    }

    pub fn sealed_at(&self) -> Timestamp {
        self.sealed_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Swept {
    pub markers: u64,
    pub chunks: u64,
}

pub(crate) async fn seal_current(
    entry: &Registered,
    tx: &mut PgConnection,
    key: &KeyBytes,
    at: Timestamp,
) -> Result<ChunkSeq, EngineError> {
    let key_value = hold_and_refuse_if_sealed(entry, tx, key).await?;
    let high_water: i64 = sqlx::query(
        "SELECT COALESCE(MAX(seq) + 1, 0) AS high_water FROM service_engine.accumulator_chunk \
         WHERE accumulator = $1 AND key = $2",
    )
    .bind(entry.name.as_str())
    .bind(&key_value)
    .fetch_one(&mut *tx)
    .await?
    .get("high_water");
    write_marker(entry, tx, &key_value, high_water, at).await?;
    Ok(ChunkSeq::from_storable(high_water.max(0)))
}

pub(crate) async fn seal_upto(
    entry: &Registered,
    tx: &mut PgConnection,
    key: &KeyBytes,
    last_seq: ChunkSeq,
    at: Timestamp,
) -> Result<ChunkSeq, EngineError> {
    let key_value = hold_and_refuse_if_sealed(entry, tx, key).await?;
    let max_seq: Option<i64> = sqlx::query(
        "SELECT MAX(seq) AS max_seq FROM service_engine.accumulator_chunk \
         WHERE accumulator = $1 AND key = $2 AND seq >= 0",
    )
    .bind(entry.name.as_str())
    .bind(&key_value)
    .fetch_one(&mut *tx)
    .await?
    .get("max_seq");
    if let Some(max) = max_seq
        && max > last_seq.to_i64()
    {
        return Err(EngineError::SealChunkBeyondLastSeq {
            accumulator: entry.name.clone(),
            last_seq: last_seq.get(),
            max_seq: ChunkSeq::from_storable(max).get(),
        });
    }
    let high_water = last_seq.to_i64().saturating_add(1);
    write_marker(entry, tx, &key_value, high_water, at).await?;
    Ok(ChunkSeq::from_storable(high_water))
}

async fn hold_and_refuse_if_sealed(
    entry: &Registered,
    tx: &mut PgConnection,
    key: &KeyBytes,
) -> Result<serde_json::Value, EngineError> {
    let stream = (entry.name.clone(), key.clone());
    guard::hold(&mut *tx, std::slice::from_ref(&stream)).await?;
    let key_value = key.decode::<serde_json::Value>()?;
    if let Some(high_water) = existing_high_water(entry, tx, &key_value).await? {
        return Err(EngineError::AlreadySealed {
            accumulator: entry.name.clone(),
            high_water: ChunkSeq::from_storable(high_water.max(0)).get(),
        });
    }
    Ok(key_value)
}

async fn existing_high_water(
    entry: &Registered,
    tx: &mut PgConnection,
    key_value: &serde_json::Value,
) -> Result<Option<i64>, EngineError> {
    Ok(sqlx::query(
        "SELECT high_water FROM service_engine.accumulator_seal \
         WHERE accumulator = $1 AND key = $2",
    )
    .bind(entry.name.as_str())
    .bind(key_value)
    .fetch_optional(&mut *tx)
    .await?
    .map(|row| row.get::<i64, _>("high_water")))
}

async fn write_marker(
    entry: &Registered,
    tx: &mut PgConnection,
    key_value: &serde_json::Value,
    high_water: i64,
    at: Timestamp,
) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM service_engine.accumulator_chunk WHERE accumulator = $1 AND key = $2")
        .bind(entry.name.as_str())
        .bind(key_value)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO service_engine.accumulator_seal (accumulator, key, high_water, sealed_at, purged_at) \
         VALUES ($1, $2, $3, $4, NULL)",
    )
    .bind(entry.name.as_str())
    .bind(key_value)
    .bind(high_water)
    .bind(at)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

pub(crate) async fn marker(
    entry: &Registered,
    pg: &PgPool,
    key: &KeyBytes,
) -> Result<Option<SealMarker>, EngineError> {
    let key_value = key.decode::<serde_json::Value>()?;
    let row = sqlx::query(
        "SELECT high_water, sealed_at FROM service_engine.accumulator_seal \
         WHERE accumulator = $1 AND key = $2",
    )
    .bind(entry.name.as_str())
    .bind(&key_value)
    .fetch_optional(pg)
    .await?;
    Ok(row.map(|row| SealMarker {
        high_water: ChunkSeq::from_storable(row.get::<i64, _>("high_water").max(0)),
        sealed_at: row.get("sealed_at"),
    }))
}

pub(crate) async fn unpurged(
    pg: &PgPool,
) -> Result<Vec<(crate::name::AccumulatorName, serde_json::Value)>, EngineError> {
    let rows = sqlx::query(
        "SELECT accumulator, key FROM service_engine.accumulator_seal \
         WHERE purged_at IS NULL ORDER BY sealed_at LIMIT 256",
    )
    .fetch_all(pg)
    .await?;
    let mut pending = Vec::with_capacity(rows.len());
    for row in &rows {
        let name = crate::name::AccumulatorName::new(row.get::<String, _>("accumulator"))?;
        pending.push((name, row.get::<serde_json::Value, _>("key")));
    }
    Ok(pending)
}

pub(crate) async fn mark_purged(
    pg: &PgPool,
    accumulator: &crate::name::AccumulatorName,
    key: &serde_json::Value,
) -> Result<(), EngineError> {
    sqlx::query(
        "UPDATE service_engine.accumulator_seal SET purged_at = now() \
         WHERE accumulator = $1 AND key = $2",
    )
    .bind(accumulator.as_str())
    .bind(key)
    .execute(pg)
    .await?;
    Ok(())
}

pub(crate) async fn sweep(
    pg: &PgPool,
    now: Timestamp,
    retention: Duration,
) -> Result<Swept, EngineError> {
    let cutoff = cutoff(now, retention);
    let markers = sqlx::query("DELETE FROM service_engine.accumulator_seal WHERE sealed_at < $1")
        .bind(cutoff)
        .execute(pg)
        .await?
        .rows_affected();
    let chunks = sqlx::query(
        "DELETE FROM service_engine.accumulator_chunk c \
         USING (SELECT accumulator, key FROM service_engine.accumulator_chunk \
                GROUP BY accumulator, key HAVING max(staged_at) < $1) abandoned \
         WHERE c.accumulator = abandoned.accumulator AND c.key = abandoned.key",
    )
    .bind(cutoff)
    .execute(pg)
    .await?
    .rows_affected();
    Ok(Swept { markers, chunks })
}

fn cutoff(now: Timestamp, retention: Duration) -> Timestamp {
    chrono::TimeDelta::from_std(retention)
        .ok()
        .and_then(|delta| now.checked_sub_signed(delta))
        .unwrap_or(Timestamp::MIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retention_the_clock_cannot_express_sweeps_nothing_rather_than_everything() {
        let now = Timestamp::from_utc(chrono::DateTime::<chrono::Utc>::MIN_UTC)
            + chrono::TimeDelta::days(1);
        assert_eq!(
            cutoff(now, Duration::from_secs(60 * 60 * 24 * 365 * 10_000)),
            Timestamp::MIN
        );
    }

    #[test]
    fn a_retention_window_is_subtracted_from_the_caller_supplied_clock() {
        let now = Timestamp::from_utc(
            chrono::DateTime::parse_from_rfc3339("2026-09-03T12:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        );
        assert_eq!(
            cutoff(now, Duration::from_secs(24 * 60 * 60))
                .as_datetime()
                .to_rfc3339(),
            "2026-09-02T12:00:00+00:00"
        );
    }
}
