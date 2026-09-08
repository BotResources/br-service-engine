#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::{attach_request, member, next_delta, upserted};
use conformance_service_engine::sample::{
    Typing, TypingView, boot_presence_engine, typing_key, typing_value, typing_window,
};
use engine_twin::{SOON, await_ready};
use uuid::Uuid;

const SERVICE: &str = "s28two";
const CHANNEL: &str = "se_s28_two";

#[tokio::test]
async fn s28_a_present_reaches_a_session_on_every_engine_that_watches_the_bucket() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let pool = db.app_pool().clone();
    let room = Uuid::now_v7();
    let typist = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;

    let engine_a = boot_presence_engine(&db, nats.nats().await, CHANNEL, "pod-a", SERVICE).await;
    let engine_b = boot_presence_engine(&db, nats.nats().await, CHANNEL, "pod-b", SERVICE).await;
    let readiness_a = engine_a.readiness();
    let readiness_b = engine_b.readiness();
    let handle_a = engine_a.presence_handle();
    let stop_a = engine_a.shutdown_handle();
    let stop_b = engine_b.shutdown_handle();

    let mut stream_a = engine_a
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches on engine A");
    let mut stream_b = engine_b
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches on engine B");

    let running_a = tokio::spawn(engine_a.run());
    let running_b = tokio::spawn(engine_b.run());
    await_ready(&readiness_a).await;
    await_ready(&readiness_b).await;

    for stream in [&mut stream_a, &mut stream_b] {
        let opening = next_delta(stream, SOON).await.expect("an opening Reset");
        assert_eq!(opening.revision().get(), 1, "the first frame is the Reset");
    }

    handle_a
        .present::<Typing>(&typing_key(room, typist), &typing_value("typing…"))
        .await
        .expect("the present writes the value into EPHEMERAL_s28two");

    for stream in [&mut stream_a, &mut stream_b] {
        let delta = next_delta(stream, SOON)
            .await
            .expect("every engine that watches the bucket delivers an Upsert");
        let view = upserted(&delta)
            .view
            .decode::<TypingView>()
            .expect("the presence view decodes");
        assert_eq!(view.room, room);
        assert_eq!(view.user, typist);
        assert_eq!(view.label, "typing…");
    }

    stop_a.notify_one();
    stop_b.notify_one();
    running_a
        .await
        .expect("engine A joins")
        .expect("engine A shuts down cleanly");
    running_b
        .await
        .expect("engine B joins")
        .expect("engine B shuts down cleanly");
    drop(nats);
    db.cleanup().await;
}
