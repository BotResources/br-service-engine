mod expr;
mod next;
mod parse;

use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::error::CronError;
use crate::name::JobName;
use crate::time::Timestamp;

pub use expr::CronExpr;
pub use next::NextFire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    EveryBeats(u32),
    Cron(CronExpr),
    Every { period: Duration, anchor: Timestamp },
}

impl Schedule {
    pub fn next_fire(&self, after: Timestamp, beat: Duration) -> Result<NextFire, CronError> {
        next::next_fire(self, after, beat)
    }

    pub fn previous_fire(&self, before: Timestamp, beat: Duration) -> Result<NextFire, CronError> {
        next::previous_fire(self, before, beat)
    }
}

pub trait CronJob: Send + Sync + 'static {
    fn name(&self) -> JobName;

    fn schedule(&self) -> Schedule;

    fn run<'a>(&'a self, pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>>;
}
