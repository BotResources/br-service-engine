use thiserror::Error;

use crate::error::BoxedError;
use crate::name::{JobName, ProjectorName};
use crate::time::Timestamp;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CronError {
    #[error("malformed cron expression {expr}: {reason}")]
    Expr { expr: String, reason: String },

    #[error("unusable schedule: {reason}")]
    Schedule { reason: String },

    #[error("a job named {name} is already registered")]
    DuplicateJob { name: JobName },

    #[error("the schedule of {job} has no fire time after {after}")]
    NoNextFire { job: JobName, after: Timestamp },

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error(transparent)]
    Job(BoxedError),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DecodeError {
    #[error("the view was produced by projector {found}, not {expected}")]
    Projector {
        expected: ProjectorName,
        found: ProjectorName,
    },

    #[error("key")]
    Key(#[source] serde_json::Error),

    #[error("view")]
    View(#[source] serde_json::Error),
}
