use std::sync::Arc;

use conformance_service_engine::inbound_support::{
    dead_letter_rows, effect_rows, publish, publish_bare, sample_subscription, start_loop,
    stub_with_effect, wait_for_dead_letter_rows, wait_for_effect_present,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{DeadLetterSource, DeadLetters, Dispatch, StubPayload, StubVerdict};
use uuid::Uuid;

#[tokio::test]
async fn s186_a_bare_frame_dead_letters_once_and_the_loop_keeps_serving() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let dispatch: Arc<dyn Dispatch> = Arc::new(stub_with_effect(db.app_pool().clone()));
    let inbound = start_loop(
        &nats.nats().await,
        vec![sample_subscription("bare-work")],
        dispatch,
        db.app_pool().clone(),
    )
    .await;

    publish_bare(
        &nats.nats().await,
        Uuid::now_v7(),
        br#"{"verdict":"commit"}"#,
    )
    .await;

    assert_eq!(
        wait_for_dead_letter_rows(db.app_pool(), 1).await,
        1,
        "a frame with no br-core-integration envelope is refused at the frontier and dead-lettered \
         rather than flowing to a handler with no sender",
    );

    let dead = DeadLetters::new(db.app_pool().clone())
        .list(Some(DeadLetterSource::Reaction), 16)
        .await
        .expect("the ops view lists the dead letters by source");
    assert_eq!(
        dead.len(),
        1,
        "exactly one dead-letter row for the bare frame"
    );
    assert_eq!(
        dead[0].source, "reaction",
        "the bare inbound frame is recorded under the reaction source",
    );
    assert_eq!(
        effect_rows(db.app_pool()).await,
        0,
        "the bare frame never reached a handler, so no effect committed",
    );

    let good = Uuid::now_v7();
    publish(
        &nats.nats().await,
        good,
        &StubPayload::of(StubVerdict::Commit),
        None,
    )
    .await;
    assert_eq!(
        wait_for_effect_present(db.app_pool(), good).await,
        1,
        "an enveloped frame published after the bare one still commits its effect: the bare frame \
         was Term'ed once and did not wedge the consumer",
    );
    assert_eq!(
        dead_letter_rows(db.app_pool()).await,
        1,
        "the good frame added no dead-letter row; the bare frame dead-lettered exactly once",
    );

    inbound.stop();
    inbound.join().await;
    db.cleanup().await;
}
