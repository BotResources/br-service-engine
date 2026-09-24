use crate::accumulator::{Accumulator, ChunkSeq, SealHash};
use crate::error::AccumulatorError;
use crate::error::EngineError;
use crate::pipeline::ops::Ops;
use crate::wire::Noun;

impl Ops<'_> {
    pub async fn accumulated<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<crate::accumulator::Accumulated<A::State>, EngineError> {
        self.accumulators.reader().state::<A>(key).await
    }

    pub async fn seal<A: Accumulator>(
        &mut self,
        key: &<A::Noun as Noun>::Key,
        last_seq: ChunkSeq,
        hash: SealHash,
    ) -> Result<A::State, EngineError> {
        self.seal_verified::<A>(key, last_seq, hash).await
    }

    pub async fn seal_partial<A: Accumulator>(
        &mut self,
        key: &<A::Noun as Noun>::Key,
        last_seq: ChunkSeq,
        hash: SealHash,
    ) -> Result<A::State, EngineError> {
        self.seal_verified::<A>(key, last_seq, hash).await
    }

    pub async fn seal_current<A: Accumulator>(
        &mut self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<A::State, EngineError> {
        let (sealed, state) = self.accumulators.seal_current::<A>(self.conn, key).await?;
        self.staged.sealed_keys.push(sealed);
        Ok(state)
    }

    async fn seal_verified<A: Accumulator>(
        &mut self,
        key: &<A::Noun as Noun>::Key,
        last_seq: ChunkSeq,
        hash: SealHash,
    ) -> Result<A::State, EngineError> {
        let (state, found) = self
            .accumulators
            .reader()
            .replay_verified::<A>(key, last_seq)
            .await?;
        if found != hash {
            return Err(EngineError::Accumulator(
                AccumulatorError::SealHashMismatch {
                    accumulator: self.accumulators.name_of::<A>()?,
                    last_seq: last_seq.get(),
                    expected: hash.to_hex(),
                    found: found.to_hex(),
                },
            ));
        }
        let sealed = self
            .accumulators
            .seal_upto::<A>(self.conn, key, last_seq)
            .await?;
        self.staged.sealed_keys.push(sealed);
        Ok(state)
    }
}
