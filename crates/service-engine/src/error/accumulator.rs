use std::time::Duration;

use thiserror::Error;

use crate::name::AccumulatorName;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AccumulatorError {
    #[error("no accumulator of type {0} is registered")]
    UnregisteredAccumulator(&'static str),

    #[error("no accumulator named {name} is registered, so an ingress chunk cannot be routed")]
    UnregisteredAccumulatorName { name: AccumulatorName },

    #[error(
        "seal_retention {seal_retention:?} does not cover the {stream} stream's max_age \
         {max_age:?}, so a straggler the stream can still redeliver would meet no seal marker"
    )]
    SealRetentionTooShort {
        stream: String,
        seal_retention: Duration,
        max_age: Duration,
    },

    #[error("accumulator name {name} is already held by another accumulator type")]
    DuplicateAccumulatorName { name: AccumulatorName },

    #[error("chunk sequence {seq} is above {max}, the largest a bigint column stores faithfully")]
    ChunkSeqOutOfRange { seq: u64, max: u64 },

    #[error(
        "chunk {seq} of accumulator {accumulator} for key {key} is already durable with different \
         content, so this submission diverges from the persisted chunk"
    )]
    ChunkConflict {
        accumulator: AccumulatorName,
        key: String,
        seq: u64,
    },

    #[error(
        "accumulator {accumulator} refuses the chunk: {limit} chunks are already waiting to be flushed"
    )]
    ChunkBufferFull {
        accumulator: AccumulatorName,
        limit: usize,
    },

    #[error("chunk {seq} of accumulator {accumulator} was abandoned before its flush committed")]
    ChunkFlushAbandoned {
        accumulator: AccumulatorName,
        seq: u64,
    },

    #[error("a fold state of another accumulator was handed to {accumulator}")]
    StateMismatch { accumulator: AccumulatorName },

    #[error(
        "chunk {seq} is refused because the key is sealed; the marker was written at high water {sealed_high_water}"
    )]
    SealedChunk { seq: u64, sealed_high_water: u64 },

    #[error(
        "accumulator {accumulator} cannot seal at last sequence {last_seq}: the stream holds a \
         contiguous prefix only up to {contiguous_to}, so it is truncated or has a gap"
    )]
    SealTruncated {
        accumulator: AccumulatorName,
        last_seq: u64,
        contiguous_to: i64,
    },

    #[error(
        "accumulator {accumulator} refuses the seal at last sequence {last_seq}: the replayed \
         chunks hash to {found} but the finish declared {expected}"
    )]
    SealHashMismatch {
        accumulator: AccumulatorName,
        last_seq: u64,
        expected: String,
        found: String,
    },

    #[error("a seal hash is not 32 lowercase-hex bytes: {0}")]
    SealHashFormat(String),

    #[error(
        "accumulator {accumulator} for this key is already sealed at high water {high_water}, so \
         sealing again would rewrite the sealed record"
    )]
    AlreadySealed {
        accumulator: AccumulatorName,
        high_water: u64,
    },

    #[error(
        "accumulator {accumulator} cannot seal at last sequence {last_seq}: the stream already \
         holds chunk {max_seq} beyond it, so a chunk was made durable that this seal would drop"
    )]
    SealChunkBeyondLastSeq {
        accumulator: AccumulatorName,
        last_seq: u64,
        max_seq: u64,
    },

    #[error(
        "an accumulator is registered but no service is configured, so the lane-A \
         STREAMING_{{service}} stream cannot be bound; call EngineConfig::with_service"
    )]
    AccumulatorWithoutService,
}
