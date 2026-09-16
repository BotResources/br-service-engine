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
mod staged;
mod tx;

pub use context::{Bulk, Mutation, PrincipalUnresolved, Reaction};
pub(crate) use dispatch::DirectPipeline;
pub use mutation::{MutationExecutor, MutationInput, MutationRegistry};
pub use ops::Ops;
pub use outbound::{OutboundCommand, OutboundEvent, ProducerSequence};
pub(crate) use outbound::{OutboundContext, event_record};
pub use policy::{PostSave, Refused};
pub(crate) use policy::{PostSavePolicies, PostSaveSeams};
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

pub trait MutationFault: Send + 'static {
    fn reason(&self) -> Option<Reason>;
}

impl MutationFault for Reason {
    fn reason(&self) -> Option<Reason> {
        Some(*self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationError {
    pub reason: Option<Reason>,
    pub detail: String,
}

impl MutationError {
    pub fn refused(reason: Option<Reason>, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self {
            reason: None,
            detail: detail.into(),
        }
    }

    pub fn code(&self) -> Option<&'static str> {
        self.reason.map(|reason| reason.code())
    }
}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            Some(reason) => write!(f, "mutation refused ({}): {}", reason.code(), self.detail),
            None => write!(f, "mutation failed: {}", self.detail),
        }
    }
}

impl std::error::Error for MutationError {}
