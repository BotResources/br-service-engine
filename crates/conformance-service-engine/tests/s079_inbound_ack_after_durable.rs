use std::sync::Arc;

use conformance_service_engine::inbound_support::{
    claim_rows, dead_letter_rows, effect_writer, publish, sample_subscription, start_loop,
    wait_for_effect_present,
};
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{Dispatch, StubDispatch, StubPayload};
use uuid::Uuid;

#[tokio::test]
async fn s079_a_crash_before_commit_redelivers_and_the_effect_lands_exactly_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let dispatch: Arc<dyn Dispatch> = Arc::new(
        StubDispatch::new(db.app_pool().clone())
            .with_effect(effect_writer())
            .crash_before_commit(1),
    );
    let inbound = start_loop(
        &nats.nats().await,
        vec![sample_subscription("crash-work")],
        dispatch,
        db.app_pool().clone(),
    )
    .await;

    let id = Uuid::now_v7();
    publish(&nats.nats().await, id, &StubPayload::commit(), None).await;

    assert_eq!(
        wait_for_effect_present(db.app_pool(), id).await,
        1,
        "the first delivery aborted before commit, the redelivery committed, so the effect is once"
    );
    assert_eq!(
        claim_rows(db.app_pool(), id).await,
        1,
        "the claim is committed with the effect, never by the aborted attempt"
    );
    assert_eq!(dead_letter_rows(db.app_pool()).await, 0);

    inbound.stop();
    inbound.join().await;
    db.cleanup().await;
}
