#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::impact::{Dims, Impact};
use uuid::Uuid;

const CHANNEL: &str = "se_s12_engine";
const BUFFER: usize = 4;

fn impacts(ids: &[Uuid]) -> Vec<Impact> {
    ids.iter().map(|id| resource(id, Dims::EMPTY)).collect()
}

#[tokio::test]
async fn s12_engine_a_lagging_session_is_reset_to_last_sent_never_silently_ended() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..8 {
        ids.push(assignment(&pool, home, &format!("row {n}")).await);
    }

    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s12").with_session_buffer(BUFFER),
        SpyAssignments::new(Spy::new()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(opening.revision().get(), 1);
    assert_eq!(reset_views(&opening).len(), 8);

    for (n, id) in ids.iter().enumerate() {
        retitle(&pool, *id, &format!("renamed {n}")).await;
    }
    stage(&pool, transport.as_ref(), &impacts(&ids)).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("a lagging session is reset, never ended");
    assert!(matches!(reset, Delta::Reset { .. }));
    assert_eq!(
        reset.revision().get(),
        2,
        "the Reset follows the revision the client last read"
    );
    let views = reset_views(&reset);
    assert_eq!(views.len(), 8);
    assert!(
        views.iter().all(
            |view| view.view.decode::<serde_json::Value>().unwrap()["title"]
                .as_str()
                .expect("a title")
                .starts_with("renamed")
        ),
        "the Reset carries last_sent, exactly the state the client is expected to hold"
    );
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the Reset replaced the buffer, it was not appended to a queue of stale deltas"
    );

    retitle(&pool, ids[0], "after the reset").await;
    stage(&pool, transport.as_ref(), &impacts(&ids[..1])).await;
    let resumed = next_delta(&mut stream, SOON)
        .await
        .expect("delivery resumes after a lag");
    assert_eq!(
        resumed.revision().get(),
        3,
        "the revision is contiguous once the session is caught up"
    );
    assert!(matches!(resumed, Delta::Upsert { .. }));

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
