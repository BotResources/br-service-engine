use std::time::Duration;

use futures_util::future::BoxFuture;
use service_engine::cron::{CronJob, Schedule};
use service_engine::error::CronError;
use service_engine::housekeeping::leader::SlotName;
use service_engine::name::JobName;
use sqlx::PgPool;
use uuid::Uuid;

pub struct SampleCronJob {
    name: JobName,
    schedule: Schedule,
    pod: String,
    hold: Duration,
}

impl SampleCronJob {
    pub fn new(name: &str, schedule: Schedule, pod: &str) -> Self {
        Self {
            name: JobName::new(name).expect("a valid job name"),
            schedule,
            pod: pod.to_string(),
            hold: Duration::ZERO,
        }
    }

    pub fn with_hold(mut self, hold: Duration) -> Self {
        self.hold = hold;
        self
    }
}

impl CronJob for SampleCronJob {
    fn name(&self) -> JobName {
        self.name.clone()
    }

    fn schedule(&self) -> Schedule {
        self.schedule.clone()
    }

    fn run<'a>(&'a self, pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async move {
            tokio::time::sleep(self.hold).await;
            sqlx::query("INSERT INTO sample_cron_run (id, job, pod) VALUES ($1, $2, $3)")
                .bind(Uuid::now_v7())
                .bind(self.name.as_str())
                .bind(&self.pod)
                .execute(pg)
                .await?;
            Ok(())
        })
    }
}

pub async fn cron_runs(pool: &PgPool, job: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_cron_run WHERE job = $1")
        .bind(job)
        .fetch_one(pool)
        .await
        .expect("count the runs a job recorded")
}

pub async fn cron_pods(pool: &PgPool, job: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT DISTINCT pod FROM sample_cron_run WHERE job = $1 ORDER BY pod")
        .bind(job)
        .fetch_all(pool)
        .await
        .expect("read which pods ran a job")
}

pub async fn claimed_slots(pool: &PgPool, job: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT slot) FROM service_engine.leader_slot WHERE name = $1",
    )
    .bind(SlotName::Cron(JobName::new(job).expect("a valid job name")).qualified())
    .fetch_one(pool)
    .await
    .expect("count the slots a job claimed")
}

pub async fn completed_slots(pool: &PgPool, job: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.leader_slot \
          WHERE name = $1 AND completed_at IS NOT NULL",
    )
    .bind(SlotName::Cron(JobName::new(job).expect("a valid job name")).qualified())
    .fetch_one(pool)
    .await
    .expect("count the slots a job completed")
}
