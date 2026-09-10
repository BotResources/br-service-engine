mod budget;
mod consumer;
mod deadletter;
mod deadletter_row;
mod dispatch;
mod disposition;
mod guard;
mod handle;
mod message;
mod reaction;
mod runtime;
mod subscription;
mod supervisor;

#[cfg(feature = "test-support")]
mod stub;

pub use budget::{
    Budgets, DEFAULT_BACKOFF_BASE, DEFAULT_BACKOFF_MAX, DEFAULT_DELIVERY_BUDGET,
    DEFAULT_PARKING_BUDGET, Route, route,
};
pub(crate) use deadletter::StagedDeadLetter;
pub use deadletter::{
    DEAD_LETTER_NOUN, DeadLetter, DeadLetterSource, DeadLetters, DiscardOutcome, RetryOutcome,
};
pub use dispatch::{Applied, Dispatch, DispatchError, DispatchOutcome, NoOp};
pub(crate) use disposition::log_reaction_db_fault;
pub use disposition::{Disposition, ReactionError, sqlx_is_terminal};
pub use guard::{Claimed, Ordering, advance_sequence, claim};
pub use message::{
    HEADER_MESSAGE_ID, HEADER_PRODUCER, HEADER_SEQ, HEADER_SEQ_KEY, Incoming, MessageMetadata,
    SequenceKey, Sequenced, Source, Unidentified,
};
pub use reaction::{ReactionEntry, ReactionInvoker, ReactionMessage, ReactionRegistry};
pub use runtime::InboundLoop;
pub use subscription::{
    DEFAULT_ACK_WAIT, DEFAULT_MAX_ACK_PENDING, InboundConfig, ReactionCoordinates, Subscription,
};
pub use supervisor::InboundHealth;
pub(crate) use supervisor::{HealthTracker, ServeExit, SupervisorConfig};

#[cfg(feature = "test-support")]
pub use stub::{DispatchCounters, EffectWriter, StubDispatch, StubPayload, StubVerdict};
