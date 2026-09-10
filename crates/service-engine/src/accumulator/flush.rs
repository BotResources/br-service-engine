use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::sync::oneshot;

use crate::accumulator::ChunkSeq;
use crate::accumulator::guard::{self, StreamKey};
use crate::accumulator::persisted::{ChunkAddress, read_persisted};
use crate::accumulator::runtime::AccumulatorRuntime;
use crate::error::EngineError;
use crate::impact::{Dims, Impact};
use crate::name::{AccumulatorName, NounName};
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

pub(crate) struct PendingChunk {
    pub accumulator: AccumulatorName,
    pub noun: NounName,
    pub key: KeyBytes,
    pub seq: ChunkSeq,
    pub chunk: serde_json::Value,
    pub done: oneshot::Sender<Result<(), EngineError>>,
}

pub(crate) type FlushBuffer = Mutex<Vec<PendingChunk>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FlushOutcome {
    pub buffered: usize,
    pub durable: usize,
    pub refused: usize,
    pub conflicts: usize,
    pub impacts: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Durable,
    Refused { sealed_high_water: u64 },
    Conflict,
    Unmapped,
}

pub(crate) async fn flush_once(
    pg: &PgPool,
    transport: &dyn ImpactTransport,
    buffer: &FlushBuffer,
    lock_timeout: Duration,
) -> Result<FlushOutcome, EngineError> {
    let batch = take(buffer);
    if batch.is_empty() {
        return Ok(FlushOutcome::default());
    }
    match commit_batch(pg, transport, &batch, lock_timeout).await {
        Ok((verdicts, outcome)) => {
            let mut deferred = Vec::new();
            for (pending, verdict) in batch.into_iter().zip(verdicts) {
                let Some(verdict) = verdict else {
                    deferred.push(pending);
                    continue;
                };
                let seq = pending.seq;
                let accumulator = pending.accumulator;
                let _ = pending.done.send(match verdict {
                    Verdict::Durable => Ok(()),
                    Verdict::Refused { sealed_high_water } => Err(EngineError::SealedChunk {
                        seq: seq.get(),
                        sealed_high_water,
                    }),
                    Verdict::Conflict => Err(EngineError::ChunkConflict {
                        accumulator,
                        key: String::from_utf8_lossy(pending.key.as_slice()).into_owned(),
                        seq: seq.get(),
                    }),
                    Verdict::Unmapped => Err(EngineError::ChunkFlushAbandoned {
                        accumulator,
                        seq: seq.get(),
                    }),
                });
            }
            if !deferred.is_empty() {
                requeue(buffer, deferred);
            }
            Ok(outcome)
        }
        Err(error) => {
            requeue(buffer, batch);
            Err(error)
        }
    }
}

pub(crate) async fn run(runtime: Arc<AccumulatorRuntime>, window: Duration, shutdown: Arc<Notify>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(window) => {
                flush_and_observe(&runtime).await;
            }
            _ = shutdown.notified() => {
                flush_and_observe(&runtime).await;
                return;
            }
        }
    }
}

async fn flush_and_observe(runtime: &AccumulatorRuntime) {
    let started = std::time::Instant::now();
    let outcome = runtime.flush_and_count().await;
    if outcome.buffered > 0 {
        crate::observe::record_chunk_flush(outcome.buffered, started.elapsed());
    }
    crate::observe::record_chunk_conflicts(outcome.conflicts);
}

fn take(buffer: &FlushBuffer) -> Vec<PendingChunk> {
    std::mem::take(&mut *buffer.lock().unwrap_or_else(|p| p.into_inner()))
}

fn requeue(buffer: &FlushBuffer, batch: Vec<PendingChunk>) {
    let mut held = buffer.lock().unwrap_or_else(|p| p.into_inner());
    let later = std::mem::replace(&mut *held, batch);
    held.extend(later);
}

async fn commit_batch(
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
