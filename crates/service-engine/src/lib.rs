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
pub mod delta;
pub mod engine;
pub mod erase;
pub mod error;
pub mod gate;
pub mod graphql;
pub mod housekeeping;
pub mod impact;
pub mod inbound;
pub mod metrics;
pub mod mirror;
pub mod name;
pub mod nats;
pub mod offer;
pub mod persistence;
pub mod pipeline;
pub mod population;
pub mod presence;
pub mod principal;
pub mod projector;
pub mod render;
pub mod schema;
pub mod scopes;
pub mod session;
pub mod time;
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
    SealMarker, Swept,
};
pub use blobs::{BlobPolicy, Blobs};
pub use cohort::CohortKey;
pub use config::EngineConfig;
pub use cron::{CronExpr, CronJob, NextFire, Schedule};
pub use delta::{Delta, ErasedView, Revision};
pub use engine::Engine;
pub use erase::{Erasable, PersonId};
pub use error::{AttachError, CronError, DecodeError, EngineError, RelayError, TransportError};
pub use gate::{
    ActionName, Affordances, Gate, GateMismatch, Gated, Reason, check_gates_match_affordances,
};
pub use graphql::DeltaKind;
pub use housekeeping::beat::{Beat, BeatRound};
pub use housekeeping::cron::{CronReport, CronRound, CronRuntime, JobRecord};
pub use housekeeping::gc::{Gc, GcRound, SessionGc};
pub use housekeeping::health::{RelayCondition, RelaysHealth, RelaysHealthReceiver};
pub use housekeeping::mirror::{
    MirrorCondition, MirrorSupervisor, MirrorTasks, MirrorsHealth, MirrorsHealthReceiver,
};
pub use housekeeping::ready::ReadinessAssembly;
#[cfg(feature = "test-support")]
pub use housekeeping::relay::{RelayRound, RelayRuntime};
pub use housekeeping::scheduled::{ScheduledBoundaries, ScheduledRound};
pub use impact::{Deps, Dims, ForeignKey, Impact, TransportEvent};
pub use inbound::{Disposition, ReactionError};
pub use mirror::MirrorHandle;
pub use name::{
    AccumulatorName, ChannelName, ForeignId, JobName, MirrorName, Namespace, NounName, PodId,
    ProjectorName, RelayName,
};
pub use nats::{
    KvBucket, KvEvent, KvKey, KvKeyError, KvPrefix, Nats, NatsError, PublishFailure,
    PublishOutcome, REASON_NO_STREAM, RelayHealth, RelayHealthReceiver,
};
pub use offer::Offer;
pub use persistence::{Persistence, PersistenceStyle};
pub use pipeline::{Bulk, Mutation, OneShot, Reaction};
pub use population::{Interest, Inverse, Population, WindowQuery};
pub use presence::Presence;
pub use principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
pub use projector::{Emission, LoadScope, Projector};
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
pub use runtime::{RenderMetrics, SessionRuntime};
pub use scopes::ScopeManifest;
pub use session::{AttachRequest, SessionId, SessionStream, WindowParams, WindowSpec};
pub use time::Timestamp;
#[cfg(feature = "test-support")]
pub use transport::{
    ImpactTransport, ListenerProbe, NOTIFY_PAYLOAD_LIMIT, PendingImpacts, PgListenNotify,
};
pub use visibility::{Cohorts, Visibility, WindowMismatch, check_window_matches_visibility};
pub use wire::{Cause, KeyBytes, Noun, ViewBytes};
