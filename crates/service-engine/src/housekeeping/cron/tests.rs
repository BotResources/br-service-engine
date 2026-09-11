use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::cron::{CronExpr, CronJob, Schedule};
use crate::error::CronError;
use crate::housekeeping::cron::{CronRuntime, DEFAULT_SLOT_RETENTION};
use crate::name::{JobName, PodId};
use crate::time::Timestamp;

struct Job {
    name: &'static str,
    schedule: Schedule,
}

impl CronJob for Job {
    fn name(&self) -> JobName {
        JobName::new(self.name).unwrap()
    }

    fn schedule(&self) -> Schedule {
        self.schedule.clone()
    }

    fn run<'a>(&'a self, _pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async { Ok(()) })
    }
}

fn runtime() -> CronRuntime {
    CronRuntime::new(PodId::new("svc-sample-0").unwrap())
}

fn at(rfc3339: &str) -> Timestamp {
    rfc3339.parse().unwrap()
}

#[test]
fn a_schedule_that_can_never_fire_is_refused_at_registration_not_at_the_first_beat() {
    let mut runtime = runtime();
    let refused = runtime
        .register(Job {
            name: "broken",
            schedule: Schedule::EveryBeats(0),
        })
        .expect_err("a job whose schedule has no period must not reach the beat");
    assert!(matches!(refused, CronError::Schedule { .. }));
    assert!(runtime.names().is_empty());
}

#[test]
fn a_schedule_whose_expression_matches_no_real_date_is_refused_instead_of_idling_forever() {
    let mut runtime = runtime();
    let refused = runtime
        .register(Job {
            name: "never",
            schedule: Schedule::Cron(CronExpr::new("0 0 30 2 *").unwrap()),
        })
        .expect_err("a job that can never fire must not sit on the board reporting idle");
    assert!(matches!(refused, CronError::NoNextFire { .. }));
    assert!(runtime.names().is_empty());
}

#[test]
fn two_jobs_of_one_name_would_share_a_slot_row_so_the_second_is_refused() {
    let mut runtime = runtime();
    runtime
        .register(Job {
            name: "nightly",
            schedule: Schedule::EveryBeats(1),
        })
        .unwrap();
    let refused = runtime
        .register(Job {
            name: "nightly",
            schedule: Schedule::Cron(CronExpr::new("0 3 * * *").unwrap()),
        })
        .expect_err("one name is one slot keyspace");
    assert!(matches!(refused, CronError::DuplicateJob { .. }));
    assert_eq!(runtime.names().len(), 1);
}

#[test]
fn the_due_slot_of_a_job_is_the_newest_fire_time_its_schedule_has_reached() {
    let mut runtime = runtime().with_beat(Duration::from_secs(1));
    runtime
        .register(Job {
            name: "nightly",
            schedule: Schedule::Cron(CronExpr::new("0 3 * * *").unwrap()),
        })
        .unwrap();
    let job = JobName::new("nightly").unwrap();
    assert_eq!(
        runtime.due_slot(&job, at("2026-01-15T09:12:00Z")),
        Some(at("2026-01-15T03:00:00Z")),
        "two pods reading their own clocks inside one day compute the same slot"
    );
    assert_eq!(
        runtime.due_slot(&job, at("2026-01-15T02:59:59Z")),
        Some(at("2026-01-14T03:00:00Z")),
        "before today's fire time the newest slot is yesterday's"
    );
    assert_eq!(
        runtime.due_slot(&JobName::new("absent").unwrap(), at("2026-01-15T09:12:00Z")),
        None
    );
}

#[test]
fn a_fresh_runtime_reports_no_run_and_a_clean_board() {
    let runtime = runtime();
    assert!(runtime.report().is_empty());
    assert!(runtime.report().is_clean());
    assert_eq!(runtime.running(), 0);
    assert_eq!(runtime.slot_retention(), DEFAULT_SLOT_RETENTION);
}

#[test]
fn a_registered_job_is_on_the_board_before_it_has_ever_run() {
    let mut runtime = runtime();
    runtime
        .register(Job {
            name: "nightly",
            schedule: Schedule::EveryBeats(1),
        })
        .unwrap();
    let record = runtime
        .report()
        .record(&JobName::new("nightly").unwrap())
        .cloned()
        .expect("the job is on the board");
    assert_eq!(record.runs, 0);
    assert!(record.is_clean());
    assert!(record.last_slot.is_none());
}
