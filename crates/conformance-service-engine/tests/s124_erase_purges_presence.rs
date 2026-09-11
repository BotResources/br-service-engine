#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{boot_erase_presence_engine, seed_note};
use conformance_service_engine::sample::render::{attach_request, member, next_delta, upserted};
use conformance_service_engine::sample::{
    Typing, TypingKey, typing_key, typing_value, typing_window,
};
use engine_twin::{SOON, await_ready};
use service_engine::PersonId;
use service_engine::delta::Delta;
use uuid::Uuid;

const SERVICE: &str = "s67presence";

fn removed(delta: &Delta) -> TypingKey {
    match delta {
        Delta::Remove { key, .. } => key.decode::<TypingKey>().expect("the removed key decodes"),
        other => panic!("expected a Remove, got {other:?}"),
    }
}

#[tokio::test]
async fn s124_erase_purges_the_persons_presence_keys_after_commit() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let note = Uuid::now_v7();
    seed_note(&pool, note, person, tenant, "mine").await;
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine =
        boot_erase_presence_engine(&db, nats.nats().await, "se_s67", "pod-s67", SERVICE).await;
    let readiness = engine.readiness();
    let handle = engine.presence_handle();
    let eraser = engine.eraser();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(&principal, vec![typing_window(note)]))
        .await
        .expect("a presence session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    handle
        .present::<Typing>(&typing_key(note, person), &typing_value("typing…"))
        .await
        .expect("the person's presence value is written");
    let upsert = next_delta(&mut stream, SOON)
        .await
        .expect("the present arrives as an Upsert");
    assert_eq!(
        upserted(&upsert).key.decode::<TypingKey>().unwrap(),
        typing_key(note, person),
    );

    eraser
        .erase(PersonId(person))
        .await
        .expect("the erase gesture runs and purges the person's presence keys");

    let remove = next_delta(&mut stream, SOON)
        .await
        .expect("purging the presence key reaches the session as a Remove");
    assert_eq!(
        removed(&remove),
        typing_key(note, person),
        "the person's presence key is gone from the bucket after the commit"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
