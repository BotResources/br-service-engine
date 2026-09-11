use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{
    boot_erase_via_delete_engine, published_note, seed_note,
};
use service_engine::{Nats, PersonId};
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const SERVICE: &str = "erasedelete";

#[tokio::test]
async fn s181_erasing_a_person_retracts_their_offered_row_through_cx_delete() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let id = Uuid::now_v7();
    seed_note(&pool, id, person, tenant, "the person's note").await;

    let engine =
        boot_erase_via_delete_engine(&db, fabric.clone(), "se_s181", "pod-s181", SERVICE).await;
    let shutdown = engine.shutdown_handle();
    let eraser = engine.eraser();
    let running = tokio::spawn(engine.run());

    await_note(
        &fabric,
        id,
        true,
        "the boot reconcile never published the person's note offer",
    )
    .await;

    let outcome = eraser
        .erase(PersonId(person))
        .await
        .expect("the erase gesture runs the cx.delete eraser in one transaction");
    assert!(outcome.fresh, "the first erase records the erasure fact");
    assert_eq!(outcome.rows_erased, 1, "the person's single note is erased");

    await_note(
        &fabric,
        id,
        false,
        "erase deleted the row through cx.delete but never retracted the offer: the engine must \
         stage the offer's dirty key in the erase transaction",
    )
    .await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_note(nats: &Nats, id: Uuid, want_present: bool, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let present = published_note(nats, id).await.is_some();
        if present == want_present {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}
