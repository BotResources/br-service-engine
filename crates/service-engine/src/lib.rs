mod advisory;
mod chain;
mod observe;
#[cfg(test)]
mod test_support;

pub mod accumulator;
pub mod blobs;
pub mod boot;
pub mod cohort;
pub mod config;
pub mod cron;
pub mod db;
pub mod delta;
pub mod dyn_compat;
pub mod engine;
pub mod erase;
pub mod error;
pub mod full_eda;
pub mod gate;
pub mod graphql;
pub mod housekeeping;
mod identity;
pub mod impact;
pub mod inbound;
pub mod lanes;
pub mod metrics;
pub mod mirror;
pub mod name;
pub mod nats;
pub mod offer;
mod offers;
pub mod persistence;
pub mod pipeline;
pub mod population;
pub mod presence;
pub mod principal;
pub mod projector;
pub mod readiness;
pub mod render;
pub mod schema;
pub mod schema_version;
pub mod scopes;
pub mod session;
pub mod stop;
pub mod time;
pub mod view;
pub mod visibility;
pub mod wire;

#[cfg(feature = "test-support")]
pub mod registry;
#[cfg(not(feature = "test-support"))]
#[allow(dead_code, unused_imports)]
mod registry;

#[cfg(feature = "test-support")]
pub mod relay;
#[cfg(not(feature = "test-support"))]
#[allow(dead_code, unused_imports)]
mod relay;

#[cfg(feature = "test-support")]
pub mod relays;
#[cfg(not(feature = "test-support"))]
#[allow(dead_code, unused_imports)]
mod relays;

#[cfg(feature = "test-support")]
pub mod runtime;
#[cfg(not(feature = "test-support"))]
#[allow(dead_code, unused_imports)]
mod runtime;

#[cfg(feature = "test-support")]
pub mod transport;
#[cfg(not(feature = "test-support"))]
#[allow(dead_code, unused_imports)]
mod transport;

pub use accumulator::{
    Accumulated, Accumulator, AccumulatorRuntime, ChunkReader, ChunkSeq, Durable, FlushOutcome,
    SealHash, SealMarker, Swept,
};
pub use blobs::{
    Blob, BlobConfig, BlobPolicy, BlobRef, Blobs, DownloadUrl, ReaperRound, UploadUrl,
};
pub use cohort::{Cohort, CohortKey, CohortValue};
pub use config::EngineConfig;
pub use cron::{CronExpr, CronJob, NextFire, Schedule};
pub use db::{connect_pool, validate_database_tls};
pub use delta::{Delta, ErasedView, Revision};
pub use engine::boot::{BootPlan, REASON_MIGRATIONS_PENDING, run_service};
pub use engine::{BlobReader, Engine, Settle};
pub use erase::{Erasable, Erase, EraseOutcome, Erased, Eraser, PersonId};
pub use error::{AttachError, CronError, DecodeError, EngineError, RelayError, TransportError};
pub use full_eda::{EventSourced, FullEda};
pub use gate::{
    ActionName, Affordances, Gate, GateMismatch, Gated, Reason, ReasonFormat,
    check_gates_match_affordances, is_reason_code,
};
pub use graphql::{
    AuthReject, CODE_EXTENSION, FORBIDDEN_CODE, GraphqlState, JsonScalar, MutationAck,
    PASSPORT_HEADER, PassportPrincipal, PrincipalRejected, Query, RootPrefix, SchemaSlices,
    SliceFragment, ack, ack_bulk, app, attach, attach_with_session, cause_json, coded_error,
    engine_schema, execute, execute_bulk, forbidden, key_json, lane_notice_stream, mutation_error,
    page, serve, typed_presence_view, typed_view,
};

pub use paste;
pub use housekeeping::beat::{Beat, BeatRound};
pub use housekeeping::cron::{CronReport, CronRound, CronRuntime, JobRecord};
pub use housekeeping::gc::{Gc, GcRound, SessionGc};
pub use housekeeping::health::{RelayCondition, RelaysHealth, RelaysHealthReceiver};
pub use housekeeping::mirror::{
    MirrorCondition, MirrorSupervisor, MirrorTasks, MirrorsHealth, MirrorsHealthReceiver,
};
pub use housekeeping::ready::{REASON_REQUIRED_KEYS, ReadinessAssembly};
#[cfg(feature = "test-support")]
pub use housekeeping::relay::{RelayRound, RelayRuntime};
pub use housekeeping::scheduled::{ScheduledBoundaries, ScheduledRound};
pub use impact::{Deps, Dims, ForeignKey, Impact, TransportEvent};
pub use inbound::{Disposition, ReactionError};
pub use lanes::{Lane, LaneNotice, LanesPaused, LanesResumed};
pub use mirror::{
    Bind, Change, ChangeOp, Column, Consumed, ConsumedGuard, ConsumedManifest, Extended, Known,
    KnownRow, KnownScope, ManifestMismatch, Mirror, MirrorHandle, MirrorKeyed, MirrorLeader,
    MirrorReady, OfferManifest, Project, Projection, Shadow, Shadows, Written, col, is_raw_json,
    manifest_key,
};
pub use name::{
    AccumulatorName, ChannelName, ForeignId, JobName, MirrorName, Namespace, NounName, PodId,
    ProjectorName, RelayName,
};
pub use nats::{
    KvBucket, KvEvent, KvKey, KvKeyError, KvPrefix, Nats, NatsCondition, NatsError, NatsHealth,
    NatsHealthChannel, NatsHealthReceiver, PublishFailure, PublishOutcome, REASON_NO_STREAM,
    RelayHealth, RelayHealthReceiver, Watched,
};
pub use offer::{Offer, OfferTrigger};
#[cfg(feature = "test-support")]
pub use offers::pause::{OfferDrainGate, arm_offer_drain, arm_offer_resolve};
pub use persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
pub use pipeline::{
    Bulk, Mutation, MutationError, MutationExecutor, MutationFault, MutationInput,
    MutationRegistry, OneShot, Ops, OutboundCommand, OutboundEvent, PostSave, ProducerSequence,
    Reaction, Refused, Saved,
};
pub use population::{Interest, Inverse, InverseLookup, Population, WindowQuery};
pub use presence::{Presence, PresenceHandle, PresenceKey, PresenceRegistry};
pub use principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
pub use projector::{Emission, LoadScope};
pub use readiness::{Readiness, ReadinessHandle, readiness_route};
#[cfg(feature = "test-support")]
pub use registry::RenderRegistry;
#[cfg(feature = "test-support")]
pub use relay::{Claim, Discipline, Drained, Relay};
#[cfg(feature = "test-support")]
pub use relays::kv::{KvChange, KvDrainRelay, KvSource, KvWrite, Versioned};
#[cfg(feature = "test-support")]
pub use relays::outbox::{HostedOutboxRelay, OutboxRelay};
pub use render::{PassReport, SessionFault, Transition};
#[cfg(feature = "test-support")]
pub use runtime::{PageReport, RenderMetrics, SessionRuntime};
pub use scopes::{ScopeError, ScopeManifest};
pub use session::{AttachRequest, SessionId, SessionStream, WindowParams, WindowSpec};
pub use stop::Stop;
pub use time::Timestamp;
#[cfg(feature = "test-support")]
pub use transport::{
    ImpactTransport, ListenerProbe, NOTIFY_PAYLOAD_LIMIT, PendingImpacts, PgListenNotify,
};
pub use view::{Populate, Projector, ViewKey, ViewProjector, cohort_window, windowed};
pub use visibility::{
    AccessReason, Cohorts, Unrestricted, Visibility, WindowMismatch,
    check_window_matches_visibility,
};
pub use wire::{Cause, KeyBytes, Noun, ViewBytes};
