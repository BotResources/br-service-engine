use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use conformance_service_engine::sample::titles::TitleProjector;
use service_engine::error::AttachError;
use service_engine::impact::Dims;
use service_engine::name::ProjectorName;
use uuid::Uuid;

const ABSENT: ProjectorName = ProjectorName::from_static("never_registered");

#[tokio::test]
async fn s03_connect_refusal() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).broken())
        .expect("the projector registers on a bound noun");
    registry
        .register_projector(TitleProjector::new(Arc::new(AtomicUsize::new(0))))
        .expect("the second projector registers on the same noun");
    let engine = runtime(&pool, render_config("pod-refusal"), registry);

    let refused = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect_err("a window whose snapshot cannot be assembled refuses the connection");
    assert!(
        matches!(&refused, AttachError::Snapshot { projector, .. } if projector == &SpyAssignments::NAME),
        "the refusal names the window that could not be assembled, got {refused}"
    );
    assert_eq!(
        engine.live_sessions().await,
        0,
        "a refused connection leaves no session behind"
    );

    let unknown = engine
        .attach(attach_request(&principal, vec![window(ABSENT, false)]))
        .await
        .expect_err("a window on a projector nobody registered is refused");
    assert!(
        matches!(&unknown, AttachError::UnknownProjector(name) if name == &ABSENT),
        "the refusal names the projector, got {unknown}"
    );
    assert_eq!(engine.live_sessions().await, 0);

    let partial = engine
        .attach(attach_request(
            &principal,
            vec![
                window(TitleProjector::NAME, false),
                window(SpyAssignments::NAME, false),
            ],
        ))
        .await
        .expect_err("one unassemblable window refuses the whole connection");
    assert!(matches!(partial, AttachError::Snapshot { .. }));
    assert_eq!(
        engine.live_sessions().await,
        0,
        "the windows that did assemble are discarded with the session"
    );

    let report = engine
        .render(vec![resource(&known, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.sessions, 0,
        "a refused connection is not a half-registered session a later pass can reach"
    );
    assert_eq!(report.deltas, 0);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(TitleProjector::NAME, false)],
        ))
        .await
        .expect("a window that does assemble still attaches");
    let first = next_delta(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("a session that opens opens with a Reset, never with an error");
    assert_eq!(first.revision().get(), 1);
    assert_eq!(reset_views(&first).len(), 1);

    db.cleanup().await;
}
