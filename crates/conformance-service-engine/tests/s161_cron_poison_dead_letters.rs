use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::{FailingCronJob, SampleCronJob, cron_runs};
use service_engine::cron::Schedule;
use service_engine::housekeeping::cron::CronRuntime;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use service_engine::name::PodId;

const BEAT: Duration = Duration::from_millis(80);

#[tokio::test]
async fn s161_a_failing_cron_tick_dead_letters_and_a_healthy_job_beside_it_keeps_running() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let dead_letters = DeadLetters::new(pool.clone());

    let mut cron = CronRuntime::new(PodId::new("se-cron-s161").expect("a valid pod id"))
        .with_beat(BEAT)
        .with_lease(Duration::from_millis(600));
    cron.set_dead_letters(dead_letters.clone());
    cron.register(FailingCronJob::new("poison", Schedule::EveryBeats(1)))
        .expect("the poison job registers");
    cron.register(SampleCronJob::new(
        "healthy",
        Schedule::EveryBeats(1),
        "se-cron-s161",
    ))
    .expect("the healthy job registers");

    for _ in 0..6 {
        cron.beat(&pool).await;
        tokio::time::sleep(BEAT).await;
    }

    assert!(
        cron_runs(&pool, "healthy").await >= 1,
        "the healthy job runs even though the poison job fails at every one of its ticks"
    );

    let dead = dead_letters
        .list(Some(DeadLetterSource::Cron), 16)
        .await
        .expect("read the dead-letter table by source");
    assert!(
        !dead.is_empty(),
        "a failing cron tick is recorded in the same dead-letter table as every other source of \
         work, so a poison tick is not silently re-run at every beat forever"
    );
    assert!(
        dead.iter().all(|row| row.source == "cron"),
        "every row listed under the Cron source is labelled cron"
    );
    assert!(
        dead.iter().any(|row| row.reaction == "poison"),
        "the poison job is the one dead-lettered"
    );

    db.cleanup().await;
}
