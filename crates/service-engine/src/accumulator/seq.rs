use crate::error::AccumulatorError;
use serde::Serialize;

use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ChunkSeq(u64);

impl ChunkSeq {
    pub const ZERO: Self = Self(0);

    pub const MAX: u64 = i64::MAX as u64;

    pub fn new(value: u64) -> Result<Self, EngineError> {
        if value > Self::MAX {
            return Err(EngineError::Accumulator(
                AccumulatorError::ChunkSeqOutOfRange {
                    seq: value,
                    max: Self::MAX,
                },
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_storable(value: i64) -> Self {
        Self(value as u64)
    }

    pub(crate) fn to_i64(self) -> i64 {
        i64::try_from(self.0).unwrap_or(i64::MAX)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub const fn follows(self, previous: Self) -> bool {
        self.0 == previous.0 + 1
    }
}

impl<'de> serde::Deserialize<'de> for ChunkSeq {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for ChunkSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_sequence_starts_at_zero_and_knows_its_successor() {
        assert_eq!(ChunkSeq::ZERO.get(), 0);
        assert!(ChunkSeq::new(1).unwrap().follows(ChunkSeq::ZERO));
        assert!(!ChunkSeq::new(2).unwrap().follows(ChunkSeq::ZERO));
        assert_eq!(ChunkSeq::ZERO.next(), ChunkSeq::new(1).unwrap());
    }

    #[test]
    fn a_chunk_sequence_a_bigint_cannot_store_faithfully_is_refused_at_construction() {
        let largest = ChunkSeq::new(ChunkSeq::MAX).expect("i64::MAX is storable");
        assert_eq!(largest.get(), i64::MAX as u64);
        assert_eq!(
            largest.to_i64(),
            i64::MAX,
            "the boundary encodes without wrapping"
        );
        let over = ChunkSeq::new(ChunkSeq::MAX + 1);
        assert!(matches!(
            over,
            Err(EngineError::Accumulator(AccumulatorError::ChunkSeqOutOfRange { seq, max }))
                if seq == (i64::MAX as u64) + 1 && max == i64::MAX as u64
        ));
        let deserialized: Result<ChunkSeq, _> =
            serde_json::from_str(&format!("{}", (i64::MAX as u64) + 1));
        assert!(
            deserialized.is_err(),
            "an out-of-range sequence cannot be smuggled in through deserialization either"
        );
    }
}
