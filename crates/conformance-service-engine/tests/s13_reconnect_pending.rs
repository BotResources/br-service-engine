use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::Gate;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(5);

#[tokio::test]
async fn s13_a_reconnect_during_connect_reopens_the_pending_session_on_a_fresh_snapshot() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let gate = Gate::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).gated_load(gate.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-reconnect-connect"), registry);

    let attaching = {
        let engine = engine.clone();
        let principal = principal.clone();
        tokio::spawn(async move {
            engine
                .attach(attach_request(
                    &principal,
                    vec![window(SpyAssignments::NAME, false)],
                ))
                .await
        })
    };

    gate.wait_until_inside().await;
    retitle(&pool, subject, "beta").await;
    engine
        .resnapshot_all()
        .await
        .expect("a transport reconnect resets sessions while one is still connecting");
    gate.release();

    let mut stream = attaching
        .await
        .expect("the attach task joins")
        .expect("attach returns a stream even though a reconnect interleaved its snapshot");

    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("the session opens with a Reset, never stale and silent");
    let views = reset_views(&opening);
    assert_eq!(views.len(), 1, "the window holds the one assignment");
    let title = views[0].view.decode::<serde_json::Value>().unwrap()["title"]
        .as_str()
        .expect("a title")
        .to_string();
    assert_eq!(
        title, "beta",
        "a session connecting across a reconnect reopens on a fresh snapshot, so it never goes \
         live holding the pre-mutation view it read before the listener came back"
    );

    db.cleanup().await;
}
