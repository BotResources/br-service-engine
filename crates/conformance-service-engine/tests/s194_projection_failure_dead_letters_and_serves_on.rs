use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(300);

#[tokio::test]
async fn s194_a_projection_failure_dead_letters_and_the_pod_keeps_serving() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let poison = Arc::new(AtomicBool::new(false));
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_poison_switch(poison.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-poison"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert!(
        matches!(opening, Delta::Reset { .. }),
        "the session opens on a healthy projection before the document turns poison",
    );

    poison.store(true, Ordering::Relaxed);
    let mut ended = false;
    for _ in 0..12 {
        let report = engine
            .render(vec![resource(&subject, Dims::EMPTY)])
            .await
            .expect("the pass runs to completion even while a projection keeps failing");
        if report.ended > 0 {
            ended = true;
            break;
        }
        assert!(
            next_delta(&mut stream, QUIET).await.is_none(),
            "a session whose view cannot be projected is never handed a Reset from stale state",
        );
    }
    assert!(
        ended,
        "a session whose projection keeps failing is repaired, then ended after the retry budget",
    );
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the ended session's stream ends explicitly rather than going silent",
    );

    let dead = DeadLetters::new(pool.clone())
        .list(Some(DeadLetterSource::Render), 16)
        .await
        .expect("the ops view lists render dead letters by source");
    assert_eq!(
        dead.len(),
        1,
        "one render dead-letter row for the poison key, deduped across every failing frame",
    );
    assert_eq!(
        dead[0].source, "render",
        "the projection failure is recorded under the render source",
    );
    assert_eq!(
        dead[0].reaction,
        SpyAssignments::NAME.as_str(),
        "the projector that could not render is the dead-letter's reaction",
    );
    assert!(
        dead[0].subject.contains(&subject.to_string()),
        "the key that could not be projected is the dead-letter's subject",
    );

    poison.store(false, Ordering::Relaxed);
    let mut healthy = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("a new session attaches after the poison frames");
    let reset = next_delta(&mut healthy, SOON)
        .await
        .expect("the healthy session still gets its Reset");
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "the runtime keeps serving new sessions after a projection failure",
    );

    db.cleanup().await;
}
