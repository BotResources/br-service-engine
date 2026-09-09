#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::{attach_request, member, next_delta, upserted};
use conformance_service_engine::sample::{
    Typing, TypingKey, boot_dual_presence_engine, typing_key, typing_value, typing_window,
};
use engine_twin::{SOON, await_ready};
use service_engine::delta::Delta;
use uuid::Uuid;

const SERVICE: &str = "s28ttl";
const CHANNEL: &str = "se_s075_ttl";

fn removed(delta: &Delta) -> TypingKey {
    match delta {
        Delta::Remove { key, .. } => key.decode::<TypingKey>().expect("the removed key decodes"),
        other => panic!("expected a Remove, got {other:?}"),
    }
}

#[tokio::test]
async fn s075_a_key_that_expires_by_ttl_leaves_every_session_as_a_remove() {
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
        boot_dual_presence_engine(&db, nats.nats().await, CHANNEL, "pod-ttl", SERVICE).await;
    let readiness = engine.readiness();
    let handle = engine.presence_handle();
    let stop = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    handle
        .present::<Typing>(&typing_key(room, typist), &typing_value("typing…"))
        .await
        .expect("the present writes the value");
    let upsert = next_delta(&mut stream, SOON)
        .await
        .expect("the present arrives as an Upsert");
    assert_eq!(
        upserted(&upsert).key.decode::<TypingKey>().unwrap(),
        typing_key(room, typist),
        "the Upsert carries the presence key"
    );

    let expiry = next_delta(&mut stream, SOON)
        .await
        .expect("the TTL expiry arrives as a Remove within the wait");
    assert_eq!(removed(&expiry), typing_key(room, typist));

    stop.notify_one();
    running
        .await
        .expect("the engine joins")
        .expect("the engine shuts down cleanly");
    drop(nats);
    db.cleanup().await;
}
