use super::*;
use crate::error::AccumulatorError;
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

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, crate::error::TransportError>> {
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
            EngineError::Accumulator(AccumulatorError::ChunkBufferFull { limit: 2, ref accumulator }) if accumulator.as_str() == "tokens"
        ),
        "{refused:?}"
    );
    assert_eq!(runtime.pending(), 2);
}
