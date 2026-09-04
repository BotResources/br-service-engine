use std::collections::BTreeMap;

use sqlx::{PgConnection, Row};

use crate::accumulator::ChunkSeq;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::wire::KeyBytes;

pub(crate) type ChunkAddress = (AccumulatorName, KeyBytes, ChunkSeq);

pub(crate) async fn read_persisted(
    tx: &mut PgConnection,
    owner: &BTreeMap<ChunkAddress, usize>,
) -> Result<BTreeMap<ChunkAddress, serde_json::Value>, EngineError> {
    if owner.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut names: Vec<String> = Vec::with_capacity(owner.len());
    let mut keys: Vec<serde_json::Value> = Vec::with_capacity(owner.len());
    let mut seqs: Vec<i64> = Vec::with_capacity(owner.len());
    for (accumulator, key, seq) in owner.keys() {
        names.push(accumulator.as_str().to_string());
        keys.push(key.decode::<serde_json::Value>()?);
        seqs.push(seq.to_i64());
    }
    let rows = sqlx::query(
        "SELECT c.accumulator, c.key, c.seq, c.chunk \
         FROM service_engine.accumulator_chunk c \
         JOIN unnest($1::text[], $2::jsonb[], $3::bigint[]) AS want(accumulator, key, seq) \
           ON c.accumulator = want.accumulator AND c.key = want.key AND c.seq = want.seq",
    )
    .bind(&names)
    .bind(&keys)
    .bind(&seqs)
    .fetch_all(&mut *tx)
    .await?;

    let mut persisted = BTreeMap::new();
    for row in rows {
        let raw_seq = row.get::<i64, _>("seq");
        if raw_seq < 0 {
            continue;
        }
        let address = (
            AccumulatorName::new(row.get::<String, _>("accumulator"))?,
            KeyBytes::encode(&row.get::<serde_json::Value, _>("key"))?,
            ChunkSeq::from_storable(raw_seq),
        );
        persisted.insert(address, row.get::<serde_json::Value, _>("chunk"));
    }
    Ok(persisted)
}
