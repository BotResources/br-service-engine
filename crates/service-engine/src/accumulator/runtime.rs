use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use tokio::sync::{Notify, oneshot};

use crate::accumulator::flush::{FlushBuffer, FlushOutcome, PendingChunk};
use crate::accumulator::reader::ChunkReader;
use crate::accumulator::seal::{SealMarker, Swept};
use crate::accumulator::{Accumulator, ChunkSeq, Durable, Registry, enroll, flush, lookup, seal};
use crate::error::EngineError;
use crate::time::{self, Timestamp};
use crate::transport::ImpactTransport;
use crate::wire::{Noun, encode_key};

pub struct AccumulatorRuntime {
    pg: PgPool,
    transport: Arc<dyn ImpactTransport>,
    registry: Registry,
    reader: ChunkReader,
    buffer: FlushBuffer,
    chunk_retention: Duration,
    max_buffered_chunks: usize,
    flush_failures: AtomicU64,
}

impl std::fmt::Debug for AccumulatorRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccumulatorRuntime")
            .field("pending", &self.pending())
            .field("max_buffered_chunks", &self.max_buffered_chunks)
            .field("chunk_retention", &self.chunk_retention)
            .finish()
    }
}

impl AccumulatorRuntime {
    pub fn new(pg: PgPool, transport: Arc<dyn ImpactTransport>, chunk_retention: Duration) -> Self {
        let registry = crate::accumulator::new_registry();
        let reader = ChunkReader::with_registry(pg.clone(), registry.clone());
        Self {
            pg,
            transport,
            registry,
            reader,
            buffer: FlushBuffer::default(),
            chunk_retention,
            max_buffered_chunks: crate::config::DEFAULT_MAX_BUFFERED_CHUNKS,
            flush_failures: AtomicU64::new(0),
        }
    }

    pub fn with_max_buffered_chunks(mut self, max_buffered_chunks: usize) -> Self {
        self.max_buffered_chunks = max_buffered_chunks;
        self
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

    pub fn chunk_retention(&self) -> Duration {
        self.chunk_retention
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
        let (done, receipt) = oneshot::channel();
        let accumulator = entry.name.clone();
        let mut held = self.buffer.lock().unwrap_or_else(|p| p.into_inner());
        if held.len() >= self.max_buffered_chunks {
            return Err(EngineError::ChunkBufferFull {
                accumulator,
                limit: self.max_buffered_chunks,
            });
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
            receipt
                .await
                .unwrap_or(Err(EngineError::ChunkFlushAbandoned {
                    accumulator,
                    seq: seq.get(),
                }))
        })))
    }

    pub async fn flush_once(&self) -> Result<FlushOutcome, EngineError> {
        flush::flush_once(&self.pg, self.transport.as_ref(), &self.buffer).await
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

    pub async fn run(self: Arc<Self>, window: Duration, shutdown: Arc<Notify>) {
        flush::run(self, window, shutdown).await
    }

    pub async fn seal<A: Accumulator>(
        &self,
        tx: &mut PgConnection,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<(), EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        seal::seal(&entry, tx, &key, time::now()).await?;
        self.reader.forget(&entry.name, &key);
        Ok(())
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
        seal::sweep(&self.pg, now, self.chunk_retention).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::{Impact, TransportEvent};
    use crate::name::{AccumulatorName, NounName};
    use crate::transport::ImpactTransport;
    use futures_util::future::BoxFuture;
    use futures_util::stream::{self, BoxStream};
    use sqlx::PgConnection;

    struct NoTransport;

    impl ImpactTransport for NoTransport {
        fn stage_in<'a>(
            &'a self,
            _conn: &'a mut PgConnection,
            _impacts: &'a [Impact],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }

        fn schedule_in<'a>(
            &'a self,
            _conn: &'a mut PgConnection,
            _noun: NounName,
            _key: crate::wire::KeyBytes,
            _at: Timestamp,
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }

        fn listen(
            &self,
        ) -> BoxStream<'static, Result<TransportEvent, crate::error::TransportError>> {
            Box::pin(stream::empty())
        }
    }

    struct Tokens;

    struct TokenNoun;

    impl Noun for TokenNoun {
        type Key = String;
        const NAME: NounName = NounName::from_static("token_stream");
    }

    impl Accumulator for Tokens {
        type Noun = TokenNoun;
        type Chunk = String;
        type State = String;

        fn name(&self) -> AccumulatorName {
            AccumulatorName::from_static("tokens")
        }

        fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
            state.push_str(&chunk);
        }
    }

    fn runtime(max_buffered_chunks: usize) -> AccumulatorRuntime {
        let pg = PgPool::connect_lazy("postgresql://engine@127.0.0.1:1/engine")
            .expect("a lazy pool never dials");
        let runtime = AccumulatorRuntime::new(pg, Arc::new(NoTransport), Duration::from_secs(60))
            .with_max_buffered_chunks(max_buffered_chunks);
        runtime.register(Tokens).expect("the accumulator enrolls");
        runtime
    }

    #[tokio::test]
    async fn a_source_faster_than_the_flush_is_refused_at_the_ceiling_instead_of_growing_the_pod() {
        let runtime = runtime(2);
        let key = "stream".to_string();
        for seq in 0..2 {
            runtime
                .push_chunk::<Tokens>(&key, ChunkSeq::new(seq).unwrap(), "token".to_string())
                .expect("the buffer accepts up to its ceiling");
        }
        let refused = runtime
            .push_chunk::<Tokens>(&key, ChunkSeq::new(2).unwrap(), "token".to_string())
            .expect_err("the chunk past the ceiling is refused");
        assert!(
            matches!(
                refused,
                EngineError::ChunkBufferFull { limit: 2, ref accumulator } if accumulator.as_str() == "tokens"
            ),
            "{refused:?}"
        );
        assert_eq!(runtime.pending(), 2);
    }
}
