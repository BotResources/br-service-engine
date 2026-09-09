use futures_util::future::BoxFuture;
use service_engine::cron::{CronJob, Schedule};
use service_engine::error::CronError;
use service_engine::name::JobName;
use sqlx::PgPool;

use super::store;

pub struct PurgeDoneCards;

impl CronJob for PurgeDoneCards {
    fn name(&self) -> JobName {
        JobName::new("purge_done_cards").expect("a valid job name")
    }

    fn schedule(&self) -> Schedule {
        Schedule::EveryBeats(10)
    }

    fn run<'a>(&'a self, pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async move {
            store::purge_done(pg)
                .await
                .map_err(|error| CronError::Job(Box::new(error)))?;
            Ok(())
        })
    }
}
