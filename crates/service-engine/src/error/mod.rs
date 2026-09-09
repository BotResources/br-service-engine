use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

use crate::name::{AccumulatorName, MirrorName, NounName, ProjectorName, RelayName};

pub type BoxedError = Box<dyn StdError + Send + Sync>;

mod codec;
pub use codec::{CronError, DecodeError};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EngineError {
    #[error("invalid {kind}: {value}")]
    InvalidName { kind: &'static str, value: String },

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("bit {0} is out of range for a 32-bit set")]
    BitOutOfRange(u8),

    #[error("encoding {what} failed")]
    Encode {
        what: &'static str,
        #[source]
        source: serde_json::Error,
    },

    #[error("decoding {what} failed")]
    Decode {
        what: &'static str,
        #[source]
        source: serde_json::Error,
    },

    #[error("projector {projector} does not decode the key type of noun {noun}")]
    NounKeyMismatch {
        projector: ProjectorName,
        noun: NounName,
    },

    #[error("projector {projector} renders noun {noun}, which no bound Noun type declares")]
    UnboundNoun {
        projector: ProjectorName,
        noun: NounName,
    },

    #[error("projector name {name} is already held by another projector")]
    DuplicateProjectorName { name: ProjectorName },

    #[error("no projector is registered under {0}")]
    UnboundProjector(ProjectorName),

    #[error("no principal resolver is registered, so a principal cannot be refreshed")]
    MissingPrincipalResolver,

    #[error("facts loaded for another projector were handed to {projector}")]
    FactsMismatch { projector: ProjectorName },

    #[error("projector {projector} emits PerImpact but the impact carries no cause")]
    CauseRequired { projector: ProjectorName },

    #[error(
        "the Query window on {projector} declares an empty Interest, so no impact can reach it"
    )]
    EmptyInterest { projector: ProjectorName },

    #[error("no accumulator of type {0} is registered")]
    UnregisteredAccumulator(&'static str),

    #[error("accumulator name {name} is already held by another accumulator type")]
    DuplicateAccumulatorName { name: AccumulatorName },

    #[error("relay name {name} is already registered, so its health board would be overwritten")]
    DuplicateRelayName { name: RelayName },

    #[error("mirror name {name} is already registered, so its health board would be overwritten")]
    DuplicateMirrorName { name: MirrorName },

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

    #[error("the {worker} worker stopped before shutdown was requested")]
    WorkerStopped { worker: &'static str },

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

    #[error("boot posture: {0}")]
    Posture(String),

    #[error("the {probe} probe did not complete within {timeout:?}")]
    ProbeTimeout {
        probe: &'static str,
        timeout: Duration,
    },

    #[error(
        "the {probe} probe heard a notification on {channel}, so the connection it probes is not exclusively its own"
    )]
    ProbeInterference {
        probe: &'static str,
        channel: String,
    },

    #[error("invalid role name: {0}")]
    InvalidRoleName(String),

    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    Nats(#[from] crate::nats::NatsError),

    #[error("database")]
    Db(#[from] sqlx::Error),

    #[error("migrations")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("service")]
    Service(#[source] BoxedError),

    #[error(transparent)]
    Scope(#[from] crate::scopes::ScopeError),

    #[error("object storage: {0}")]
    Blob(String),

    #[error(
        "object-storage bucket {bucket} is absent (HEAD answered {status}); gitops declares the \
         bucket, the engine only binds it"
    )]
    BlobBucketAbsent { bucket: String, status: u16 },

    #[error("blob kind {kind} is used but was never registered with register_blobs")]
    BlobKindUnregistered { kind: &'static str },

    #[error("presence type `{presence}` is used but was never registered with register_presence")]
    PresenceNotRegistered { presence: String },

    #[error("http server on {addr}: {source}")]
    Http {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },

    #[error("two slices contribute the same graphql {kind} `{member}`: {first} and {second}")]
    DuplicateSchemaMember {
        kind: &'static str,
        member: String,
        first: &'static str,
        second: &'static str,
    },

    #[error(
        "the composed schema exposes the graphql root field `{member}` that no slice fragment declared"
    )]
    UndeclaredSchemaMember { member: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransportError {
    #[error("listener connection")]
    Listen(#[source] sqlx::Error),

    #[error("staging impacts")]
    Stage(#[source] sqlx::Error),

    #[error("impact payload")]
    Payload(#[source] serde_json::Error),

    #[error("a single impact renders to {size} bytes, over the {limit}-byte NOTIFY payload limit")]
    PayloadTooLarge { size: usize, limit: usize },

    #[error("malformed impact frame: {0}")]
    Frame(String),

    #[error(
        "the impact listener was already taken; listen() is single-use so a second, unprobed \
         listener never starts"
    )]
    ListenerConsumed,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AttachError {
    #[error("no projector is registered under {0}")]
    UnknownProjector(ProjectorName),

    #[error("the window on {projector} asks for RLS but no RlsApplier is registered")]
    MissingRlsApplier { projector: ProjectorName },

    #[error(
        "the window on {projector} was attached with rls={requested} but the projector renders \
         under rls={declared}; the render regime is the projector's, not the call's"
    )]
    RlsRegimeMismatch {
        projector: ProjectorName,
        declared: bool,
        requested: bool,
    },

    #[error("attaching a window requires a registered PrincipalResolver")]
    MissingPrincipalResolver,

    #[error("the Query window on {projector} declares an empty Interest")]
    EmptyInterest { projector: ProjectorName },

    #[error("the snapshot of the window on {projector} could not be assembled")]
    Snapshot {
        projector: ProjectorName,
        #[source]
        source: EngineError,
    },

    #[error("the principal no longer exists, so the session it was attaching was ended")]
    PrincipalRevoked,

    #[error("the impacts held while the session was connecting could not be rendered")]
    HeldImpacts(#[source] EngineError),

    #[error("the connection did not assemble its snapshot within {after:?}")]
    ConnectTimedOut { after: Duration },

    #[error("the engine is shutting down")]
    ShuttingDown,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RelayError {
    #[error("database")]
    Db(#[from] sqlx::Error),

    #[error("publishing a claimed row: {0}")]
    Publish(String),

    #[error("relay")]
    Relay(#[source] BoxedError),
}
