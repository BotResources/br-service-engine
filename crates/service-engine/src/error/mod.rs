use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

use crate::name::{AccumulatorName, MirrorName, NounName, ProjectorName, RelayName};
use crate::session::SessionId;

pub type BoxedError = Box<dyn StdError + Send + Sync>;

mod codec;
pub use codec::{CronError, DecodeError};

mod attach;
mod transport;
pub use attach::AttachError;
pub use transport::{RelayError, TransportError};

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

    #[error(
        "view {view} declares Unrestricted visibility with an empty reason; \
         state why no cohort gate applies (open_access!)"
    )]
    EmptyAccessReason { view: ProjectorName },

    #[error("no principal resolver is registered, so a principal cannot be refreshed")]
    MissingPrincipalResolver,

    #[error("facts loaded for another projector were handed to {projector}")]
    FactsMismatch { projector: ProjectorName },

    #[error("projector {projector} could not project key {key}")]
    Projection {
        projector: ProjectorName,
        key: String,
        #[source]
        source: BoxedError,
    },

    #[error(
        "the Query window on {projector} declares an empty Interest, so no impact can reach it"
    )]
    EmptyInterest { projector: ProjectorName },

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

    #[error("relay name {name} is already registered, so its health board would be overwritten")]
    DuplicateRelayName { name: RelayName },

    #[error("mirror name {name} is already registered, so its health board would be overwritten")]
    DuplicateMirrorName { name: MirrorName },

    #[error(
        "mirror {mirror} consumes prefix {prefix} as a raw serde_json::Value; a consumed value \
         must be typed, or set Consumed::RAW_JSON_ESCAPE_HATCH when the producer's column is \
         itself JSON"
    )]
    RawJsonConsumption {
        mirror: MirrorName,
        prefix: &'static str,
    },

    #[error(
        "mirror {mirror} requires key {key}, which is outside its consumed prefix {prefix}; a \
         required key must live under a prefix the mirror consumes"
    )]
    RequiredKeyOutsidePrefix {
        mirror: MirrorName,
        prefix: &'static str,
        key: &'static str,
    },

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

    #[error("boot posture: {0}")]
    Posture(String),

    #[error(
        "schema version conflict: a live pod runs engine {live_engine} / service {live_service}, \
         this pod is engine {engine_version} / service {service_version}"
    )]
    SchemaVersionConflict {
        live_engine: String,
        live_service: String,
        engine_version: String,
        service_version: String,
    },

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

    #[error(
        "a verified upload of blob kind {kind} expects {size} bytes, over the {max_bytes}-byte \
         policy ceiling; the expectation is refused before any presign is minted"
    )]
    BlobOverPolicy {
        kind: &'static str,
        size: u64,
        max_bytes: u64,
    },

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

    #[error("a policy refused the write with reason {code}")]
    PolicyRefused { code: &'static str },

    #[error(
        "create refused: an aggregate already exists under key {key} in store {store}; \
         a creator-generated id is used once and the engine never overwrites through create"
    )]
    KeyReused { store: &'static str, key: String },

    #[error(
        "store {store} does not implement Persistence::delete, so the engine cannot hard-delete \
         one of its aggregates through cx.delete"
    )]
    DeleteUnsupported { store: &'static str },

    #[error(
        "a slice declared aggregate `{aggregate}` subject to a post-save policy, but no slice \
         registered one; the seam is unhonoured, so the write path it must guard would run \
         unguarded"
    )]
    UnhonouredSeam { aggregate: &'static str },

    #[error(
        "the composed schema exposes the graphql object type `{ty}` that no slice fragment owns \
         and the engine does not inject"
    )]
    UndeclaredSchemaType { ty: String },

    #[error("the composed graphql schema could not be parsed for slice verification: {detail}")]
    SchemaParse { detail: String },

    #[error(
        "no live session {session} on this pod, so its window cannot be paged; a page request \
         must be issued over the session's own connection, which pins it to the pod that holds it"
    )]
    NoLiveSession { session: SessionId },

    #[error("session {session} holds no window on projector {projector} to page")]
    NoSuchWindow {
        session: SessionId,
        projector: ProjectorName,
    },

    #[error(
        "the store is not fully migrated (engine set pending: {engine}, service set pending: \
         {service}); serve refuses to run until migrate has applied both sets to the shared ledger"
    )]
    MigrationsPending { engine: bool, service: bool },
}
