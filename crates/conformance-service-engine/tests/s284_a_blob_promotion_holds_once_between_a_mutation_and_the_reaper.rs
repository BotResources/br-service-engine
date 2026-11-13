#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;
#[allow(dead_code)]
mod promote_support;

use std::time::Duration;

use conformance_service_engine::sample::{PromotePolicy, boot_blob_promote_engine};
use promote_support::{Rig, Running};
use service_engine::{BlobPolicy, BlobPromotion};

const FAST_REAPER: Duration = Duration::from_millis(50);

async fn wait_uploaded(rig: &Rig, reference: uuid::Uuid) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while rig.state(reference).await.0 != "uploaded" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper never promoted the row"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn s284a_a_mutation_finds_a_row_the_reaper_promoted_already_uploaded() {
    let rig = Rig::new("s284a").await;
    let running = Running::start(
        rig.engine_with_reaper("s284a", PromotePolicy::Probe, FAST_REAPER)
            .await,
    )
    .await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    rig.land(upload).await;
    wait_uploaded(&rig, reference).await;
    assert_eq!(
        rig.probes(reference).await.len(),
        1,
        "the reaper ran the policy"
    );

    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(answer, BlobPromotion::AlreadyUploaded);
    let probes = rig.probes(reference).await;
    assert_eq!(probes.len(), 1, "the mutation did not run the policy again");
    assert_eq!(
        probes[0]["metadata"]["actor_kind"], "service",
        "the reaper's emit goes out as the service too"
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s284b_the_reaper_skips_a_row_a_mutation_promoted() {
    let rig = Rig::new("s284b").await;
    let running = Running::start(rig.engine("s284b", PromotePolicy::Probe).await).await;
    let (doc, reference, upload) = rig.attach(&running.executor).await;
    rig.land(upload).await;
    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(answer, BlobPromotion::Promoted);
    running.stop().await;

    let mut engine = boot_blob_promote_engine(
        &rig.db,
        rig.nats.nats().await,
        "se_s284b",
        "pod-s284b-reaper",
        rig.minio.config(&rig.bucket),
        BlobPolicy {
            max_bytes: 1 << 20,
            orphan_after: Duration::from_secs(3600),
        },
        FAST_REAPER,
        PromotePolicy::Probe,
    )
    .await;
    let rounds = engine.observe_blob_reaper_rounds();
    let running = Running::start(engine).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while rounds
        .lock()
        .unwrap()
        .iter()
        .filter(|round| round.ran)
        .count()
        < 3
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper never swept"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        rounds
            .lock()
            .unwrap()
            .iter()
            .all(|round| round.promoted == 0 && round.failed == 0),
        "the reaper finds no pending row to promote",
    );
    assert_eq!(rig.state(reference).await.0, "uploaded");
    assert_eq!(rig.probes(reference).await.len(), 1, "the policy ran once");

    rig.finish(running).await;
}

#[tokio::test]
async fn s284c_a_mutation_racing_the_reaper_runs_the_policy_exactly_once() {
    const BLOBS: usize = 8;
    let rig = Rig::new("s284c").await;
    let mut engine = rig
        .engine_with_reaper("s284c", PromotePolicy::Probe, FAST_REAPER)
        .await;
    let rounds = engine.observe_blob_reaper_rounds();
    let running = Running::start(engine).await;

    let mut attached = Vec::new();
    for _ in 0..BLOBS {
        let (doc, reference, upload) = rig.attach(&running.executor).await;
        attached.push((doc, reference, upload));
    }
    for (_, _, upload) in &attached {
        rig.land(upload.clone()).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    'sweeping: loop {
        for (_, reference, _) in &attached {
            if rig.state(*reference).await.0 == "uploaded" {
                break 'sweeping;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper never started promoting"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let answers = futures_util::future::join_all(
        attached
            .iter()
            .map(|(doc, _, _)| rig.promote(&running.executor, *doc)),
    )
    .await;

    let mut by_mutation = 0;
    for answer in answers {
        match answer.expect("a promote mutation racing the reaper commits") {
            BlobPromotion::Promoted => by_mutation += 1,
            BlobPromotion::AlreadyUploaded => {}
            other => {
                panic!("a landed, matching blob answers Promoted or AlreadyUploaded: {other:?}")
            }
        }
    }
    for (_, reference, _) in &attached {
        wait_uploaded(&rig, *reference).await;
    }
    let ran_before = rounds
        .lock()
        .unwrap()
        .iter()
        .filter(|round| round.ran)
        .count();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while rounds
        .lock()
        .unwrap()
        .iter()
        .filter(|round| round.ran)
        .count()
        < ran_before + 2
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reaper stopped sweeping"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for (_, reference, _) in &attached {
        assert_eq!(
            rig.probes(*reference).await.len(),
            1,
            "the post-upload policy ran exactly once for blob {reference}",
        );
    }
    let by_reaper: u64 = rounds
        .lock()
        .unwrap()
        .iter()
        .map(|round| round.promoted)
        .sum();
    assert!(
        by_reaper >= 1,
        "the mutations raced a reaper sweep already under way"
    );
    assert_eq!(
        by_mutation as u64 + by_reaper,
        BLOBS as u64,
        "every row was promoted once, by the mutation or by the reaper, never by both",
    );

    rig.finish(running).await;
}
