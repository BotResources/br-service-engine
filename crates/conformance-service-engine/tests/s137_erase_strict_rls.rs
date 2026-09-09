#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{
    boot_erase_strict_engine, count_secrets, seed_secret,
};
use engine_twin::await_ready;
use service_engine::PersonId;
use uuid::Uuid;

const SERVICE: &str = "s126strict";

#[tokio::test]
async fn s137_erase_deletes_rows_under_a_strict_deny_when_unset_rls_policy() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let other = Uuid::now_v7();

    let mine_a = Uuid::now_v7();
    let mine_b = Uuid::now_v7();
    let theirs = Uuid::now_v7();
    seed_secret(&pool, mine_a, person, tenant, "one").await;
    seed_secret(&pool, mine_b, person, tenant, "two").await;
    seed_secret(&pool, theirs, other, tenant, "theirs").await;

    assert_eq!(
        count_secrets(&pool, tenant, person).await,
        2,
        "the person owns two rows on a table with FORCE ROW LEVEL SECURITY and no fail-open branch"
    );

    let engine =
        boot_erase_strict_engine(&db, nats.nats().await, "se_s126", "pod-s126", SERVICE).await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let eraser = engine.eraser();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let outcome = eraser
        .erase(PersonId(person))
        .await
        .expect("erase runs against the strict-RLS table");
    assert!(outcome.fresh, "the first erase records the erasure fact");
    assert_eq!(
        outcome.rows_erased, 2,
        "the strict policy admits the delete only because the engine set app.erasing; with no \
         context and no whitelist the low-privilege role would see zero rows and erase nothing"
    );

    assert_eq!(
        count_secrets(&pool, tenant, person).await,
        0,
        "the person's strict-RLS rows are gone after the commit"
    );
    assert_eq!(
        count_secrets(&pool, tenant, other).await,
        1,
        "erase is owner-scoped, so another person on the same tenant is untouched"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
