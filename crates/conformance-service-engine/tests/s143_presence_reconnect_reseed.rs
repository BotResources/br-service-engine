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

const SERVICE: &str = "s143recon";
const CHANNEL: &str = "se_s143_recon";

fn reset_has_typing(delta: &Delta, key: &TypingKey) -> Option<bool> {
    match delta {
        Delta::Reset { views, .. } => Some(views.iter().any(|view| {
            view.projector == Typing::NAME
                && view.key.decode::<TypingKey>().ok().as_ref() == Some(key)
        })),
        _ => None,
    }
}

#[tokio::test]
async fn s143_a_key_that_expires_while_nats_is_down_is_repaired_by_a_reseed_and_reset_on_return() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let pool = db.app_pool().clone();
    let room = Uuid::now_v7();
    let typist = Uuid::now_v7();
    let key = typing_key(room, typist);
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;

    let engine =
        boot_dual_presence_engine(&db, nats.nats().await, CHANNEL, "pod-recon", SERVICE).await;
    let readiness = engine.readiness();
    let handle = engine.presence_handle();
    let stop = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches on the typing window");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    handle
        .present::<Typing>(&key, &typing_value("typing…"))
        .await
        .expect("the value is written with its 2s ttl");
    let upsert = next_delta(&mut stream, SOON)
        .await
        .expect("the present arrives as an Upsert");
    assert_eq!(
        upserted(&upsert).key.decode::<TypingKey>().unwrap(),
        key,
        "the Upsert carries the presence key before the blink"
    );

    nats.stop();
    tokio::time::sleep(Duration::from_secs(5)).await;
    nats.restart().await;

    let deadline = tokio::time::Instant::now() + SOON;
    let mut saw_reset = false;
    let mut key_present = true;
    while tokio::time::Instant::now() < deadline && !(saw_reset && !key_present) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Some(delta) = next_delta(&mut stream, remaining).await else {
            break;
        };
        if let Some(has_key) = reset_has_typing(&delta, &key) {
            saw_reset = true;
            key_present = has_key;
        } else if let Delta::Remove {
            projector,
            key: removed,
            ..
        } = &delta
        {
            if projector == &Typing::NAME
                && removed.decode::<TypingKey>().ok().as_ref() == Some(&key)
            {
                key_present = false;
            }
        }
    }

    assert!(
        saw_reset,
        "on the watch re-open after the reconnect the pod pushes a projector reset, so the \
         session receives a fresh Reset"
    );
    assert!(
        !key_present,
        "the key that expired while NATS was down is gone after the reconnect: the reseed reads \
         it out of the bucket instead of serving the stale value forever"
    );

    stop.notify_one();
    running
        .await
        .expect("the engine joins")
        .expect("the engine shuts down cleanly");
    drop(nats);
    db.cleanup().await;
}
