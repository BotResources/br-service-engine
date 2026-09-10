use std::time::Duration;

use thiserror::Error;

use super::EngineError;
use crate::name::ProjectorName;
use crate::session::SessionId;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AttachError {
    #[error("no projector is registered under {0}")]
    UnknownProjector(ProjectorName),

    #[error("the window on {projector} asks for RLS but no RlsApplier is registered")]
    MissingRlsApplier { projector: ProjectorName },

    #[error(
        "the window on {projector} was attached with rls={requested} but the projector renders \
         under rls={declared}; the render regime is the projector's, not the call's"
    )]
    RlsRegimeMismatch {
        projector: ProjectorName,
        declared: bool,
        requested: bool,
    },

    #[error("attaching a window requires a registered PrincipalResolver")]
    MissingPrincipalResolver,

    #[error("the Query window on {projector} declares an empty Interest")]
    EmptyInterest { projector: ProjectorName },

    #[error("the snapshot of the window on {projector} could not be assembled")]
    Snapshot {
        projector: ProjectorName,
        #[source]
        source: EngineError,
    },

    #[error("the principal no longer exists, so the session it was attaching was ended")]
    PrincipalRevoked,

    #[error("the impacts held while the session was connecting could not be rendered")]
    HeldImpacts(#[source] EngineError),

    #[error("the connection did not assemble its snapshot within {after:?}")]
    ConnectTimedOut { after: Duration },

    #[error("the engine is shutting down")]
    ShuttingDown,

    #[error("a live session already holds the id {session}")]
    DuplicateSession { session: SessionId },
}
