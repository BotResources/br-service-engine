use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use tokio::sync::oneshot;

use crate::accumulator::flush::{FlushBuffer, FlushOutcome, PendingChunk};
use crate::accumulator::reader::ChunkReader;
use crate::accumulator::seal::{SealMarker, Swept};
use crate::accumulator::{Accumulator, ChunkSeq, Durable, Registry, enroll, flush, lookup, seal};
use crate::chain::describe;
use crate::error::{AccumulatorError, EngineError};
use crate::name::AccumulatorName;
use crate::nats::{Nats, chunk_subject, streaming_stream, subject_token};
use crate::stop::Stop;
use crate::time::{self, Timestamp};
use crate::transport::ImpactTransport;
use crate::wire::{KeyBytes, Noun, encode_key};

struct LaneAPurge {
    nats: Nats,
    service: String,
}

pub struct AccumulatorRuntime {
    pg: PgPool,
    transport: Arc<dyn ImpactTransport>,
    registry: Registry,
    reader: ChunkReader,
    buffer: FlushBuffer,
    seal_retention: Duration,
    max_buffered_chunks: usize,
    flush_lock_timeout: Duration,
    flush_failures: AtomicU64,
    lane_a: OnceLock<LaneAPurge>,
}

impl std::fmt::Debug for AccumulatorRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccumulatorRuntime")
            .field("pending", &self.pending())
            .field("max_buffered_chunks", &self.max_buffered_chunks)
            .field("seal_retention", &self.seal_retention)
            .finish()
    }
}

impl AccumulatorRuntime {
    pub fn new(pg: PgPool, transport: Arc<dyn ImpactTransport>, seal_retention: Duration) -> Self {
        let registry = crate::accumulator::new_registry();
        let reader = ChunkReader::with_registry(pg.clone(), registry.clone());
        Self {
            pg,
            transport,
            registry,
            reader,
            buffer: FlushBuffer::default(),
            seal_retention,
            max_buffered_chunks: crate::config::DEFAULT_MAX_BUFFERED_CHUNKS,
            flush_lock_timeout: crate::config::DEFAULT_LOCK_TIMEOUT,
            flush_failures: AtomicU64::new(0),
            lane_a: OnceLock::new(),
        }
    }

    pub fn with_max_buffered_chunks(mut self, max_buffered_chunks: usize) -> Self {
        self.max_buffered_chunks = max_buffered_chunks;
        self
    }

    pub fn with_flush_lock_timeout(mut self, flush_lock_timeout: Duration) -> Self {
        self.flush_lock_timeout = flush_lock_timeout;
        self
    }

    pub(crate) fn bind_lane_a_purge(&self, nats: Nats, service: String) {
        let _ = self.lane_a.set(LaneAPurge { nats, service });
    }

    pub fn with_fold_cache_capacity(mut self, capacity: usize) -> Self {
        self.reader.set_capacity(capacity);
        self
    }

    pub fn register<A: Accumulator>(&self, accumulator: A) -> Result<(), EngineError> {
        enroll(&self.registry, accumulator)
    }

    pub fn reader(&self) -> &ChunkReader {
        &self.reader
    }

    pub fn seal_retention(&self) -> Duration {
        self.seal_retention
    }

    pub fn max_buffered_chunks(&self) -> usize {
        self.max_buffered_chunks
    }

    pub fn pending(&self) -> usize {
        self.buffer.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Relaxed)
    }

    pub fn push_chunk<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
        seq: ChunkSeq,
        chunk: A::Chunk,
    ) -> Result<Durable, EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        let chunk = serde_json::to_value(&chunk).map_err(|source| EngineError::Encode {
            what: "chunk",
            source,
        })?;
        self.enqueue(entry, key, seq, chunk)
    }

    pub fn push_frame(
        &self,
        accumulator: &crate::name::AccumulatorName,
        key: &serde_json::Value,
        seq: ChunkSeq,
        chunk: serde_json::Value,
    ) -> Result<Durable, EngineError> {
        let entry = crate::accumulator::lookup_by_name(&self.registry, accumulator)?;
        let key = crate::wire::KeyBytes::encode(key)?;
        self.enqueue(entry, key, seq, chunk)
    }

    pub fn registered(&self) -> usize {
        crate::accumulator::registered_count(&self.registry)
    }

    fn enqueue(
        &self,
        entry: crate::accumulator::Registered,
        key: crate::wire::KeyBytes,
        seq: ChunkSeq,
        chunk: serde_json::Value,
    ) -> Result<Durable, EngineError> {
        let (done, receipt) = oneshot::channel();
        let accumulator = entry.name.clone();
        let mut held = self.buffer.lock().unwrap_or_else(|p| p.into_inner());
        if held.len() >= self.max_buffered_chunks {
            return Err(EngineError::Accumulator(
                AccumulatorError::ChunkBufferFull {
                    accumulator,
                    limit: self.max_buffered_chunks,
                },
            ));
        }
        held.push(PendingChunk {
            accumulator: entry.name,
            noun: entry.noun,
            key,
            seq,
            chunk,
            done,
        });
        drop(held);
        Ok(Durable::new(Box::pin(async move {
            receipt.await.unwrap_or(Err(EngineError::Accumulator(
                AccumulatorError::ChunkFlushAbandoned {
                    accumulator,
                    seq: seq.get(),
                },
            )))
        })))
    }

    pub async fn flush_once(&self) -> Result<FlushOutcome, EngineError> {
        flush::flush_once(
            &self.pg,
            self.transport.as_ref(),
            &self.buffer,
            self.flush_lock_timeout,
        )
        .await
    }

    pub(crate) async fn flush_and_count(&self) -> FlushOutcome {
        match self.flush_once().await {
            Ok(outcome) => outcome,
            Err(_) => {
                self.flush_failures.fetch_add(1, Ordering::Relaxed);
                FlushOutcome::default()
            }
        }
    }

    pub async fn run(self: Arc<Self>, window: Duration, shutdown: Arc<Stop>) {
        flush::run(self, window, shutdown).await
    }

    pub async fn seal_current<A: Accumulator>(
        &self,
        tx: &mut PgConnection,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<((AccumulatorName, KeyBytes), A::State), EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        let (_high_water, state) = seal::seal_current(&entry, tx, &key, time::now()).await?;
        self.reader.forget(&entry.name, &key);
        let state = *state.downcast::<A::State>().map_err(|_| {
            EngineError::Accumulator(AccumulatorError::StateMismatch {
                accumulator: entry.name.clone(),
            })
        })?;
        Ok(((entry.name, key), state))
    }

    #[cfg(feature = "test-support")]
    pub fn stream_lock_id<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<i64, EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        Ok(crate::accumulator::guard::lock_id(&(entry.name, key)))
    }

    pub async fn seal_upto<A: Accumulator>(
        &self,
        tx: &mut PgConnection,
        key: &<A::Noun as Noun>::Key,
        last_seq: ChunkSeq,
    ) -> Result<(AccumulatorName, KeyBytes), EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        seal::seal_upto(&entry, tx, &key, last_seq, time::now()).await?;
        self.reader.forget(&entry.name, &key);
        Ok((entry.name, key))
    }

    pub(crate) async fn purge_committed_seals(&self, sealed: &[(AccumulatorName, KeyBytes)]) {
        let Some(lane) = self.lane_a.get() else {
            return;
        };
        let stream = streaming_stream(&lane.service);
        for (accumulator, key) in sealed {
            if let Err(error) = purge_one(
                &lane.nats,
                &lane.service,
                &stream,
                &self.pg,
                accumulator,
                key,
            )
            .await
            {
                tracing::warn!(
                    reason = %describe(&error),
                    "the sealing pod could not purge the lane-A subject synchronously; the beat is the backstop"
                );
            }
        }
    }

    pub fn name_of<A: Accumulator>(&self) -> Result<crate::name::AccumulatorName, EngineError> {
        Ok(lookup::<A>(&self.registry)?.name)
    }

    pub async fn sealed<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<Option<SealMarker>, EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        seal::marker(&entry, &self.pg, &key).await
    }

    pub async fn sweep_expired(&self, now: Timestamp) -> Result<Swept, EngineError> {
        seal::sweep(&self.pg, now, self.seal_retention).await
    }
}

async fn purge_one(
    nats: &Nats,
    service: &str,
    stream: &str,
    pg: &PgPool,
    accumulator: &AccumulatorName,
    key: &KeyBytes,
) -> Result<(), EngineError> {
    let key_value = key.decode::<serde_json::Value>()?;
    let token = subject_token(&key_value)?;
    nats.purge_chunk_subject(stream, &chunk_subject(service, &token))
        .await?;
    seal::mark_purged(pg, accumulator, &key_value).await?;
    Ok(())
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
