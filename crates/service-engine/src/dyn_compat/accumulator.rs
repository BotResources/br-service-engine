use std::any::Any;
use std::sync::Arc;

use crate::accumulator::{Accumulator, ChunkSeq};
use crate::error::EngineError;
use crate::name::{AccumulatorName, NounName};
use crate::wire::Noun;

pub type ErasedState = Box<dyn Any + Send + Sync>;

pub trait ErasedAccumulator: Send + Sync + 'static {
    fn name(&self) -> AccumulatorName;

    fn noun(&self) -> NounName;

    fn init_state(&self) -> ErasedState;

    fn fold(
        &self,
        state: &mut ErasedState,
        seq: ChunkSeq,
        chunk: &serde_json::Value,
    ) -> Result<(), EngineError>;
}

pub struct AccumulatorAdapter<A>(A);

impl<A> AccumulatorAdapter<A> {
    pub fn new(accumulator: A) -> Self {
        Self(accumulator)
    }

    pub fn inner(&self) -> &A {
        &self.0
    }
}

pub fn erase_accumulator<A: Accumulator>(accumulator: A) -> Arc<dyn ErasedAccumulator> {
    Arc::new(AccumulatorAdapter::new(accumulator))
}

impl<A: Accumulator> ErasedAccumulator for AccumulatorAdapter<A> {
    fn name(&self) -> AccumulatorName {
        self.0.name()
    }

    fn noun(&self) -> NounName {
        <A::Noun as Noun>::NAME
    }

    fn init_state(&self) -> ErasedState {
        Box::new(A::State::default())
    }

    fn fold(
        &self,
        state: &mut ErasedState,
        seq: ChunkSeq,
        chunk: &serde_json::Value,
    ) -> Result<(), EngineError> {
        let typed_state =
            state
                .downcast_mut::<A::State>()
                .ok_or_else(|| EngineError::StateMismatch {
                    accumulator: self.0.name(),
                })?;
        let chunk: A::Chunk =
            serde_json::from_value(chunk.clone()).map_err(|source| EngineError::Decode {
                what: "chunk",
                source,
            })?;
        self.0.fold(typed_state, seq, chunk);
        Ok(())
    }
}
