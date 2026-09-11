use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sqlx::PgPool;

use crate::accumulator::ChunkSeq;
use crate::accumulator::flush::{FlushOutcome, PendingChunk, Verdict};
use crate::accumulator::guard::{self, StreamKey};
use crate::accumulator::persisted::{ChunkAddress, read_persisted};
use crate::error::EngineError;
use crate::impact::{Dims, Impact};
use crate::name::NounName;
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

pub(crate) async fn commit_batch(
    pg: &PgPool,
    transport: &dyn ImpactTransport,
    batch: &[PendingChunk],
    lock_timeout: Duration,
) -> Result<(Vec<Option<Verdict>>, FlushOutcome), EngineError> {
    let mut owner: BTreeMap<ChunkAddress, usize> = BTreeMap::new();
    let mut diverged: BTreeSet<ChunkAddress> = BTreeSet::new();
    for (index, pending) in batch.iter().enumerate() {
        let address = (
            pending.accumulator.clone(),
            pending.key.clone(),
            pending.seq,
        );
        match owner.get(&address) {
            None => {
                owner.insert(address, index);
            }
            Some(&first) => {
                if batch[first].chunk != pending.chunk {
                    diverged.insert(address);
                }
            }
        }
    }
    let streams: Vec<StreamKey> = owner
        .keys()
        .map(|(accumulator, key, _)| (accumulator.clone(), key.clone()))
        .collect::<BTreeSet<StreamKey>>()
        .into_iter()
        .collect();

    let mut tx = pg.begin().await?;
    set_lock_timeout(&mut tx, lock_timeout).await?;
    let held = guard::try_hold(&mut tx, &streams).await?;
    let acquired: BTreeMap<StreamKey, bool> =
        streams.iter().cloned().zip(held.iter().copied()).collect();
    let acquired_streams: Vec<StreamKey> = streams
        .iter()
        .zip(held.iter().copied())
        .filter(|&(_, held)| held)
        .map(|(stream, _)| stream.clone())
        .collect();
    let stream_index: BTreeMap<&StreamKey, usize> = acquired_streams
        .iter()
        .enumerate()
        .map(|(index, stream)| (stream, index))
        .collect();
    let acquired_owner: BTreeMap<ChunkAddress, usize> = owner
        .iter()
        .filter(|((accumulator, key, _), _)| {
            acquired
                .get(&(accumulator.clone(), key.clone()))
                .copied()
                .unwrap_or(false)
        })
        .map(|(address, index)| (address.clone(), *index))
        .collect();

    let sealed = guard::read_seals(&mut tx, &acquired_streams).await?;
    let existing = read_persisted(&mut tx, &acquired_owner).await?;

    let mut accumulators: Vec<String> = Vec::new();
    let mut keys: Vec<serde_json::Value> = Vec::new();
    let mut seqs: Vec<i64> = Vec::new();
    let mut chunks: Vec<serde_json::Value> = Vec::new();
    let mut touched: BTreeSet<(NounName, KeyBytes)> = BTreeSet::new();
    let mut verdict_of: BTreeMap<ChunkAddress, Verdict> = BTreeMap::new();

    for (address, index) in &acquired_owner {
        let pending = &batch[*index];
        let stream = (pending.accumulator.clone(), pending.key.clone());
        let position = stream_index[&stream];
        if let Some(sealed_high_water) = sealed[position] {
            verdict_of.insert(address.clone(), Verdict::Refused { sealed_high_water });
            continue;
        }
        if let Some(persisted) = existing.get(address) {
            let verdict = if diverged.contains(address) || persisted != &pending.chunk {
                Verdict::Conflict
            } else {
                Verdict::Durable
            };
            verdict_of.insert(address.clone(), verdict);
            continue;
        }
        if diverged.contains(address) {
            verdict_of.insert(address.clone(), Verdict::Conflict);
            continue;
        }
        accumulators.push(pending.accumulator.as_str().to_string());
        keys.push(pending.key.decode::<serde_json::Value>()?);
        seqs.push(i64::try_from(pending.seq.get()).map_err(|_| {
            EngineError::ChunkSeqOutOfRange {
                seq: pending.seq.get(),
                max: ChunkSeq::MAX,
            }
        })?);
        chunks.push(pending.chunk.clone());
        touched.insert((pending.noun.clone(), pending.key.clone()));
        verdict_of.insert(address.clone(), Verdict::Durable);
    }

    if !accumulators.is_empty() {
        sqlx::query(
            "INSERT INTO service_engine.accumulator_chunk (accumulator, key, seq, chunk) \
             SELECT * FROM unnest($1::text[], $2::jsonb[], $3::bigint[], $4::jsonb[]) \
             ON CONFLICT (accumulator, key, seq) DO NOTHING",
        )
        .bind(&accumulators)
        .bind(&keys)
        .bind(&seqs)
        .bind(&chunks)
        .execute(&mut *tx)
        .await?;
    }

    let impacts: Vec<Impact> = touched
        .into_iter()
        .map(|(noun, key)| Impact::ResourceChanged {
            noun,
            key,
            dims: Dims::EMPTY,
            cause: None,
        })
        .collect();
    if !impacts.is_empty() {
        transport.stage_in(&mut tx, &impacts).await?;
    }

    tx.commit().await?;
    crate::observe::record_impacts_committed(impacts.len());

    let verdicts: Vec<Option<Verdict>> = batch
        .iter()
        .map(|pending| {
            let stream = (pending.accumulator.clone(), pending.key.clone());
            if !acquired.get(&stream).copied().unwrap_or(false) {
                return None;
            }
            let address = (
                pending.accumulator.clone(),
                pending.key.clone(),
                pending.seq,
            );
            Some(
                verdict_of
                    .get(&address)
                    .copied()
                    .unwrap_or(Verdict::Unmapped),
            )
        })
        .collect();
    let processed = verdicts.iter().filter(|verdict| verdict.is_some()).count();
    let durable = verdicts
        .iter()
        .filter(|verdict| matches!(verdict, Some(Verdict::Durable)))
        .count();
    let conflicts = verdicts
        .iter()
        .filter(|verdict| matches!(verdict, Some(Verdict::Conflict)))
        .count();
    let outcome = FlushOutcome {
        buffered: processed,
        durable,
        refused: processed - durable - conflicts,
        conflicts,
        impacts: impacts.len(),
    };
    Ok((verdicts, outcome))
}

async fn set_lock_timeout(
    conn: &mut sqlx::PgConnection,
    lock_timeout: Duration,
) -> Result<(), EngineError> {
    let millis = lock_timeout.as_millis().max(1);
    sqlx::query(&format!("SET LOCAL lock_timeout = {millis}"))
        .execute(conn)
        .await?;
    Ok(())
}
