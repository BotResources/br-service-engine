use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

pub const NOT_FOUND: Reason = Reason::new("NOT_FOUND");

#[derive(Debug, thiserror::Error)]
pub enum AppFault {
    #[error("refused: {}", .0.code())]
    Refused(Reason),
    #[error("the aggregate does not exist")]
    NotFound,
    #[error("the store failed")]
    Store(#[from] EngineError),
}

impl MutationFault for AppFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::NotFound => Some(NOT_FOUND),
            Self::Store(_) => None,
        }
    }
}

impl From<Reason> for AppFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionFault {
    #[error("not yet: {0}")]
    NotYet(String),
    #[error("terminal: {0}")]
    Terminal(String),
    #[error("the message is malformed")]
    Malformed(#[source] EngineError),
    #[error("the store failed")]
    Store(#[from] EngineError),
}

impl ReactionError for ReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::NotYet(_) => Disposition::Park,
            Self::Terminal(_) | Self::Malformed(_) => Disposition::Terminal,
            Self::Store(EngineError::Db(db)) if sqlx_is_terminal(db) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

impl From<sqlx::Error> for ReactionFault {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(EngineError::from(error))
    }
}

impl From<AppFault> for ReactionFault {
    fn from(fault: AppFault) -> Self {
        match fault {
            AppFault::Refused(reason) => Self::Terminal(reason.code().to_string()),
            AppFault::NotFound => Self::NotYet("aggregate absent".into()),
            AppFault::Store(error) => Self::Store(error),
        }
    }
}
