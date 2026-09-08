#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::{attach_request, member, next_delta, reset_views};
use conformance_service_engine::sample::{
    Typing, TypingView, boot_presence_engine, typing_key, typing_value, typing_window,
};
use engine_twin::{SOON, await_ready};
use uuid::Uuid;

const SERVICE: &str = "s28reset";
const CHANNEL_A: &str = "se_s28_reset_a";
const CHANNEL_B: &str = "se_s28_reset_b";

#[tokio::test]
async fn s28_a_session_that_attaches_after_a_present_finds_it_in_the_reset() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let pool = db.app_pool().clone();
    let room = Uuid::now_v7();
    let typist = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;

    let engine_a = boot_presence_engine(&db, nats.nats().await, CHANNEL_A, "pod-a", SERVICE).await;
    let readiness_a = engine_a.readiness();
    let handle_a = engine_a.presence_handle();
    let stop_a = engine_a.shutdown_handle();
    let running_a = tokio::spawn(engine_a.run());
    await_ready(&readiness_a).await;

    handle_a
        .present::<Typing>(&typing_key(room, typist), &typing_value("typing…"))
        .await
        .expect("the present writes the value into the shared bucket");

    let engine_b = boot_presence_engine(&db, nats.nats().await, CHANNEL_B, "pod-b", SERVICE).await;
    let readiness_b = engine_b.readiness();
    let stop_b = engine_b.shutdown_handle();
    let render_b = engine_b.render();
    let running_b = tokio::spawn(engine_b.run());
    await_ready(&readiness_b).await;

    let mut stream_b = render_b
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches on the second engine after it is ready and seeded");

    let opening = next_delta(&mut stream_b, SOON)
        .await
        .expect("the opening Reset");
    let views = reset_views(&opening);
    assert_eq!(views.len(), 1, "the Reset carries the one current presence");
    let view = views[0]
        .view
        .decode::<TypingView>()
        .expect("the presence view decodes");
    assert_eq!(view.room, room);
    assert_eq!(view.user, typist);
    assert_eq!(view.label, "typing…");

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
