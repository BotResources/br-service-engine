use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, AccumulatorRuntime, ChunkSeq, Durable};
use service_engine::error::EngineError;
use service_engine::name::AccumulatorName;

use crate::sample::note::{Note, NoteKey};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteBodyState {
    pub text: String,
    pub folded: Vec<u64>,
}

pub struct NoteBody;

impl NoteBody {
    pub const NAME: AccumulatorName = AccumulatorName::from_static("note_body");
}

impl Accumulator for NoteBody {
    type Noun = Note;
    type Chunk = String;
    type State = NoteBodyState;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut NoteBodyState, seq: ChunkSeq, chunk: String) {
        state.text.push_str(&chunk);
        state.folded.push(seq.get());
    }
}

pub struct SyntheticSource<'a> {
    runtime: &'a AccumulatorRuntime,
    key: NoteKey,
}

impl<'a> SyntheticSource<'a> {
    pub fn new(runtime: &'a AccumulatorRuntime, key: NoteKey) -> Self {
        Self { runtime, key }
    }

    pub fn emit(&self, seq: u64, token: &str) -> Result<Durable, EngineError> {
        self.runtime
            .push_chunk::<NoteBody>(&self.key, ChunkSeq::new(seq)?, token.to_string())
    }

    pub fn emit_range(
        &self,
        seqs: std::ops::RangeInclusive<u64>,
    ) -> Result<Vec<Durable>, EngineError> {
        seqs.map(|seq| self.emit(seq, &token_for(seq))).collect()
    }
}

pub fn token_for(seq: u64) -> String {
    format!("<{seq}>")
}

pub fn text_for(seqs: impl IntoIterator<Item = u64>) -> String {
    seqs.into_iter().map(token_for).collect()
}

pub fn note_body_runtime(
    pg: sqlx::PgPool,
    transport: std::sync::Arc<dyn service_engine::transport::ImpactTransport>,
    chunk_retention: std::time::Duration,
) -> std::sync::Arc<AccumulatorRuntime> {
    let runtime = AccumulatorRuntime::new(pg, transport, chunk_retention);
    runtime
        .register(NoteBody)
        .expect("the note body accumulator enrolls");
    std::sync::Arc::new(runtime)
}
