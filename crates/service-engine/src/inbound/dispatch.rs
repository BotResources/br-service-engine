use futures_util::future::BoxFuture;

use crate::inbound::disposition::Disposition;
use crate::inbound::message::Incoming;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoOp {
    Duplicate,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Committed,
    NoOp(NoOp),
}

#[derive(Debug, Clone)]
pub struct DispatchError {
    pub disposition: Disposition,
    pub detail: String,
}

impl DispatchError {
    pub fn new(disposition: Disposition, detail: impl Into<String>) -> Self {
        Self {
            disposition,
            detail: detail.into(),
        }
    }

    pub fn retry(detail: impl Into<String>) -> Self {
        Self::new(Disposition::Retry, detail)
    }

    pub fn park(detail: impl Into<String>) -> Self {
        Self::new(Disposition::Park, detail)
    }

    pub fn terminal(detail: impl Into<String>) -> Self {
        Self::new(Disposition::Terminal, detail)
    }
}

#[derive(Debug, Clone)]
pub enum DispatchOutcome {
    Applied(Applied),
    Failed(DispatchError),
}

pub trait Dispatch: Send + Sync + 'static {
    fn dispatch<'a>(&'a self, msg: &'a Incoming) -> BoxFuture<'a, DispatchOutcome>;
}
