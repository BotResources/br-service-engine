use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::oneshot;

use crate::accumulator::ChunkSeq;
use crate::accumulator::commit::commit_batch;
use crate::accumulator::runtime::AccumulatorRuntime;
use crate::error::{AccumulatorError, EngineError};
use crate::name::{AccumulatorName, NounName};
use crate::stop::Stop;
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
pub(crate) enum Verdict {
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
                    Verdict::Refused { sealed_high_water } => {
                        Err(EngineError::Accumulator(AccumulatorError::SealedChunk {
                            seq: seq.get(),
                            sealed_high_water,
                        }))
                    }
                    Verdict::Conflict => {
                        Err(EngineError::Accumulator(AccumulatorError::ChunkConflict {
                            accumulator,
                            key: String::from_utf8_lossy(pending.key.as_slice()).into_owned(),
                            seq: seq.get(),
                        }))
                    }
                    Verdict::Unmapped => Err(EngineError::Accumulator(
                        AccumulatorError::ChunkFlushAbandoned {
                            accumulator,
                            seq: seq.get(),
                        },
                    )),
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

pub(crate) async fn run(runtime: Arc<AccumulatorRuntime>, window: Duration, shutdown: Arc<Stop>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(window) => {
                flush_and_observe(&runtime).await;
            }
            _ = shutdown.stopped() => {
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
