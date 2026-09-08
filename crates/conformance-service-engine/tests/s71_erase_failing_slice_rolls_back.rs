use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{
    boot_failing_erase_engine, count_ledger_by_author, count_memos, count_notes, seed_ledger,
    seed_memo, seed_note,
};
use service_engine::PersonId;
use sqlx::PgPool;
use uuid::Uuid;

const SERVICE: &str = "erasefail";

async fn erasure_recorded(pool: &PgPool, person: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.person_erasure WHERE person_id = $1")
        .bind(person)
        .fetch_one(pool)
        .await
        .expect("count the erasure fact rows")
}

async fn person_erased_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM integration_outbox WHERE subject = 'integration.evt.{SERVICE}.person.erased.v1'"
    ))
    .fetch_one(pool)
    .await
    .expect("count the PersonErased outbox rows")
}

#[tokio::test]
async fn s66_a_slice_that_fails_its_erase_rolls_the_whole_gesture_back() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();

    let note = Uuid::now_v7();
    seed_note(&pool, note, person, tenant, "mine").await;
    let memo = Uuid::now_v7();
    seed_memo(&pool, memo, person, tenant, "soft body").await;
    let ledger = Uuid::now_v7();
    seed_ledger(&pool, ledger, tenant, person, 3).await;

    let engine = boot_failing_erase_engine(&db, fabric, "se_s66", "pod-s66", SERVICE).await;
    let eraser = engine.eraser();

    let result = eraser.erase(PersonId(person)).await;
    assert!(
        result.is_err(),
        "one slice failing makes the whole erase fail loudly"
    );

    assert_eq!(
        count_notes(&pool, person).await,
        1,
        "the CRUD note that an earlier slice deleted is restored by the rollback"
    );
    assert_eq!(
        count_memos(&pool, person).await,
        1,
        "the soft memo survives"
    );
    assert_eq!(
        count_ledger_by_author(&pool, person).await,
        2,
        "the full-EDA event and snapshot still name the person"
    );
    assert_eq!(
        erasure_recorded(&pool, person).await,
        0,
        "the erasure fact is rolled back with the effect"
    );
    assert_eq!(
        person_erased_rows(&pool).await,
        0,
        "no PersonErased is left behind by a rolled-back erase"
    );

    drop(engine);
    drop(nats);
    db.cleanup().await;
}
