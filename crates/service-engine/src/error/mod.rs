use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

use crate::name::{MirrorName, NounName, ProjectorName, RelayName};

pub type BoxedError = Box<dyn StdError + Send + Sync>;

pub use crate::chain::describe;

mod codec;
pub use codec::{CronError, DecodeError};

mod accumulator;
mod attach;
mod composition;
mod transport;
pub use accumulator::AccumulatorError;
pub use attach::AttachError;
pub use composition::CompositionError;
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
        "mirror {mirror} requires key {key}, which is outside what it consumes as {prefix}; a \
         required key must live under a consumed prefix, or be the consumed key itself"
    )]
    RequiredKeyOutsidePrefix {
        mirror: MirrorName,
        prefix: &'static str,
        key: &'static str,
    },

    #[error("the {worker} worker stopped before shutdown was requested")]
    WorkerStopped { worker: &'static str },

    #[error("boot posture: {0}")]
    Posture(String),

    #[error(
        "owner posture: migrate refuses the owner role {role}, which is neither a superuser nor \
         BYPASSRLS; a migration run by a role subject to row-level security touches no row of a \
         FORCE ROW LEVEL SECURITY table and still reports success, so declare the owner role with \
         BYPASSRLS"
    )]
    OwnerSubjectToRls { role: String },

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

    #[error(
        "invalid schema name {0}: a library schema must be a lowercase Postgres identifier and \
         may not be public or service_engine"
    )]
    InvalidSchemaName(String),

    #[error("two libraries contribute the same name or schema: {name}")]
    DuplicateLibrary { name: String },

    #[error(
        "migration bands overlap between {first} and {second}; every library band and the engine's \
         reserved range must be pairwise disjoint"
    )]
    MigrationBandOverlap {
        first: &'static str,
        second: &'static str,
    },

    #[error("migration {version} of library {owner} falls outside the band the library declares")]
    MigrationOutsideBand { owner: &'static str, version: i64 },

    #[error(
        "library {library} created relation {object} outside its declared schema; a library owns \
         exactly one schema and may not touch public or another library's"
    )]
    LibraryMigrationEscapedSchema {
        library: &'static str,
        object: String,
    },

    #[error(
        "service migration {version} falls inside the reserved band of {owner}; service migrations \
         must sit outside every reserved band"
    )]
    ServiceMigrationInReservedBand { version: i64, owner: &'static str },

    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    Nats(#[from] crate::nats::NatsError),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error(transparent)]
    Service(BoxedError),

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

    #[error(
        "blob kind {kind} sets orphan_after {orphan_after:?}, shorter than the upload TTL \
         {upload_ttl:?}; an upload that lands after an incomplete row is reaped would orphan an \
         object no sweep sees, so the kind is refused at bind"
    )]
    BlobOrphanWindowTooShort {
        kind: &'static str,
        orphan_after: Duration,
        upload_ttl: Duration,
    },

    #[error("presence type `{presence}` is used but was never registered with register_presence")]
    PresenceNotRegistered { presence: String },

    #[error("http server on {addr}")]
    Http {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },

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
        "the store is not fully migrated (engine set pending: {engine}, pending libraries: \
         {libraries:?}, service set pending: {service}); serve refuses to run until migrate has \
         applied the engine, library and service sets to the shared ledger"
    )]
    MigrationsPending {
        engine: bool,
        libraries: Vec<&'static str>,
        service: bool,
    },

    #[error(transparent)]
    Accumulator(#[from] AccumulatorError),

    #[error(transparent)]
    Composition(#[from] CompositionError),
}
