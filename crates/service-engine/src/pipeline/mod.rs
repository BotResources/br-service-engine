mod classify;
mod confirm;
mod context;
mod dispatch;
mod effect;
mod mutation;
mod ops;
mod ops_blob;
mod ops_seal;
mod outbound;
mod policy;
mod runner;
mod staged;
mod tx;

pub use context::{Bulk, Mutation, PrincipalUnresolved, Reaction};
pub(crate) use dispatch::DirectPipeline;
pub use mutation::{MutationExecutor, MutationInput, MutationRegistry};
pub use ops::Ops;
pub use outbound::{OutboundCommand, OutboundEvent, ProducerSequence};
pub(crate) use outbound::{OutboundContext, event_record};
pub(crate) use policy::{AggregatePolicies, Policies, PolicySeams};
pub use policy::{PostSave, Refused, Saved};
pub(crate) use runner::{PolicyRunner, PromotionOutcome};
pub(crate) use staged::Staged;
pub(crate) use tx::{begin_scoped, flush_and_commit};

use crate::gate::Reason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneShot<T>(pub T);

impl<T> OneShot<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn get(&self) -> &T {
        &self.0
    }
}

pub trait MutationFault: std::error::Error + Send + 'static {
    fn reason(&self) -> Option<Reason>;
}

impl MutationFault for Reason {
    fn reason(&self) -> Option<Reason> {
        Some(*self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationError(Outcome);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Refused { reason: Reason, detail: String },
    Failed(InternalCause),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
struct InternalCause(String);

impl MutationError {
    pub fn refused(reason: Reason, detail: impl Into<String>) -> Self {
        Self(Outcome::Refused {
            reason,
            detail: detail.into(),
        })
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        let detail = detail.into();
        tracing::error!(
            %detail,
            "a mutation failed on an internal fault; the client receives INTERNAL while the \
             cause is kept here"
        );
        Self::failed(detail)
    }

    pub(crate) fn failed(detail: impl Into<String>) -> Self {
        Self(Outcome::Failed(InternalCause(detail.into())))
    }

    pub fn reason(&self) -> Option<Reason> {
        match &self.0 {
            Outcome::Refused { reason, .. } => Some(*reason),
            Outcome::Failed(_) => None,
        }
    }

    pub fn code(&self) -> Option<&'static str> {
        self.reason().map(|reason| reason.code())
    }

    pub fn refusal_detail(&self) -> Option<&str> {
        match &self.0 {
            Outcome::Refused { detail, .. } => Some(detail),
            Outcome::Failed(_) => None,
        }
    }
}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Outcome::Refused { reason, detail } => {
                write!(f, "mutation refused ({}): {detail}", reason.code())
            }
            Outcome::Failed(_) => f.write_str("mutation failed"),
        }
    }
}

impl std::error::Error for MutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            Outcome::Refused { .. } => None,
            Outcome::Failed(cause) => Some(cause),
        }
    }
}

#[cfg(test)]
#[path = "mutation_error_tests.rs"]
mod mutation_error_tests;
