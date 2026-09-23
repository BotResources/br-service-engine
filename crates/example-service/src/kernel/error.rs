use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

/// The aggregate a mutation names does not exist: a refusal the client can
/// act on, so it carries a code like any other.
pub const NOT_FOUND: Reason = Reason::new("NOT_FOUND");

/// A mutation's error. A refusal carries its reason code; a store failure
/// carries no reason, reaches the client as `INTERNAL`, and keeps the engine
/// error's whole cause chain as text, which the engine logs through
/// `as_error`.
#[derive(Debug)]
pub enum AppFault {
    Refused(Reason),
    NotFound,
    Store(String),
}

impl std::fmt::Display for AppFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {}", reason.code()),
            Self::NotFound => f.write_str("the aggregate does not exist"),
            Self::Store(cause) => write!(f, "store: {cause}"),
        }
    }
}

impl std::error::Error for AppFault {}

impl MutationFault for AppFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::NotFound => Some(NOT_FOUND),
            Self::Store(_) => None,
        }
    }

    fn as_error(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self)
    }
}

impl From<Reason> for AppFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

impl From<EngineError> for AppFault {
    fn from(error: EngineError) -> Self {
        Self::Store(service_engine::error::describe(&error))
    }
}

#[derive(Debug)]
pub enum ReactionFault {
    NotYet(String),
    Terminal(String),
    Store(String),
}

impl std::fmt::Display for ReactionFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotYet(detail) => write!(f, "not yet: {detail}"),
            Self::Terminal(detail) => write!(f, "terminal: {detail}"),
            Self::Store(detail) => write!(f, "store: {detail}"),
        }
    }
}

impl std::error::Error for ReactionFault {}

impl ReactionError for ReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::NotYet(_) => Disposition::Park,
            Self::Terminal(_) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

impl From<EngineError> for ReactionFault {
    fn from(error: EngineError) -> Self {
        match &error {
            EngineError::Db(db) if sqlx_is_terminal(db) => {
                Self::Terminal(service_engine::error::describe(&error))
            }
            _ => Self::Store(service_engine::error::describe(&error)),
        }
    }
}

impl From<sqlx::Error> for ReactionFault {
    fn from(error: sqlx::Error) -> Self {
        Self::from(EngineError::from(error))
    }
}

impl From<AppFault> for ReactionFault {
    fn from(fault: AppFault) -> Self {
        match fault {
            AppFault::Refused(reason) => Self::Terminal(reason.code().to_string()),
            AppFault::NotFound => Self::NotYet("aggregate absent".into()),
            AppFault::Store(cause) => Self::Store(cause),
        }
    }
}
