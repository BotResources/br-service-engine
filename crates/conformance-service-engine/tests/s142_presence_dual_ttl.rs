#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::{attach_request, member, next_delta};
use conformance_service_engine::sample::{
    Cursor, Typing, boot_dual_presence_engine, cursor_key, cursor_value, cursor_window, typing_key,
    typing_value, typing_window,
};
use engine_twin::{SOON, await_ready};
use service_engine::delta::Delta;
use uuid::Uuid;

const SERVICE: &str = "s142dual";
const CHANNEL: &str = "se_s142_dual";

#[tokio::test]
async fn s142_two_lanes_with_different_ttls_in_one_bucket_each_expire_at_their_own_ttl() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let pool = db.app_pool().clone();
    let room = Uuid::now_v7();
    let typist = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;

    let engine =
        boot_dual_presence_engine(&db, nats.nats().await, CHANNEL, "pod-dual", SERVICE).await;
    let readiness = engine.readiness();
    let handle = engine.presence_handle();
    let stop = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![typing_window(room), cursor_window(room)],
        ))
        .await
        .expect("a session attaches on both presence windows");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    handle
        .present::<Typing>(&typing_key(room, typist), &typing_value("typing…"))
        .await
        .expect("the fast typing value is written with its own 2s ttl");
    handle
        .present::<Cursor>(&cursor_key(room, typist), &cursor_value(7))
        .await
        .expect("the slow cursor value is written with its own 6s ttl");

    let mut saw_typing_upsert = false;
    let mut saw_cursor_upsert = false;
    while !(saw_typing_upsert && saw_cursor_upsert) {
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("both presents arrive as Upserts");
        match &delta {
            Delta::Upsert { view, .. } if view.projector == Typing::NAME => {
                saw_typing_upsert = true;
            }
            Delta::Upsert { view, .. } if view.projector == Cursor::NAME => {
                saw_cursor_upsert = true;
            }
            other => panic!("expected a typing or cursor Upsert, got {other:?}"),
        }
    }

    let first_removed = next_delta(&mut stream, SOON)
        .await
        .expect("the fast lane's key expires first, within the wait");
    match &first_removed {
        Delta::Remove { projector, .. } => assert_eq!(
            projector,
            &Typing::NAME,
            "the 2s typing lane expires before the 6s cursor lane, proving each key carries its \
             own ttl rather than the single bucket max_age"
        ),
        other => panic!("expected the typing Remove first, got {other:?}"),
    }

    let second_removed = next_delta(&mut stream, SOON)
        .await
        .expect("the slow lane's key expires at its own, later ttl");
    match &second_removed {
        Delta::Remove { projector, .. } => assert_eq!(
            projector,
            &Cursor::NAME,
            "the cursor lane outlived the typing lane and expired at its own 6s ttl"
        ),
        other => panic!("expected the cursor Remove second, got {other:?}"),
    }

    stop.notify_one();
    running
        .await
        .expect("the engine joins")
        .expect("the engine shuts down cleanly");
    drop(nats);
    db.cleanup().await;
}
