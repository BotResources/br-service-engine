#[allow(dead_code)]
mod engine_twin;

use br_util_axum_readiness::ReadinessHandle;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::{
    SampleCronJob, claimed_slots, completed_slots, cron_pods, cron_runs,
};
use engine_twin::{POLL, SOON, await_ready};
use service_engine::Engine;
use service_engine::cron::Schedule;

const CHANNEL: &str = "se_s16_engine";
const JOB: &str = "s16_beat";
const POD: &str = "pod-s16";

#[tokio::test]
async fn s16_engine_a_cron_job_runs_once_per_slot_on_the_beat() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, POD),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .register_cron(SampleCronJob::new(JOB, Schedule::EveryBeats(1), POD))
        .expect("the sample cron job registers");
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        if completed_slots(&pool, JOB).await >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the beat never drove the cron job through two leader slots"
        );
        tokio::time::sleep(POLL).await;
    }

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");

    let runs = cron_runs(&pool, JOB).await;
    let completed = completed_slots(&pool, JOB).await;
    let claimed = claimed_slots(&pool, JOB).await;
    assert!(
        completed >= 2,
        "the job completed at least two slots, got {completed}"
    );
    assert!(
        runs <= claimed,
        "no slot ran more than once: runs={runs} must not exceed the claimed slots={claimed}"
    );
    assert!(
        runs >= completed,
        "every completed slot recorded its one run: runs={runs} completed={completed}"
    );
    assert_eq!(
        cron_pods(&pool, JOB).await,
        vec![POD.to_string()],
        "a single engine holds every slot it claims"
    );

    drop(nats);
    db.cleanup().await;
}
