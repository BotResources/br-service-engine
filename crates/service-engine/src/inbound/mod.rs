mod budget;
mod consumer;
mod deadletter;
mod dispatch;
mod disposition;
mod guard;
mod message;
mod reaction;
mod runtime;
mod subscription;

#[cfg(feature = "test-support")]
mod stub;

pub use budget::{
    Budgets, DEFAULT_BACKOFF_BASE, DEFAULT_BACKOFF_MAX, DEFAULT_DELIVERY_BUDGET,
    DEFAULT_PARKING_BUDGET, Route, route,
};
pub use deadletter::{DeadLetter, DeadLetterSource, DeadLetters, DiscardOutcome, RetryOutcome};
pub use dispatch::{Applied, Dispatch, DispatchError, DispatchOutcome, NoOp};
pub use disposition::{Disposition, ReactionError, sqlx_is_terminal};
pub use guard::{Claimed, Ordering, advance_sequence, claim};
pub use message::{
    HEADER_MESSAGE_ID, HEADER_PRODUCER, HEADER_SEQ, HEADER_SEQ_KEY, Incoming, SequenceKey,
    Sequenced, Source, Unidentified,
};
pub use reaction::{ReactionEntry, ReactionInvoker, ReactionMessage, ReactionRegistry};
pub use runtime::InboundLoop;
pub use subscription::{
    DEFAULT_ACK_WAIT, DEFAULT_MAX_ACK_PENDING, InboundConfig, ReactionCoordinates, Subscription,
};

#[cfg(feature = "test-support")]
pub use stub::{DispatchCounters, EffectWriter, StubDispatch, StubPayload, StubVerdict};
