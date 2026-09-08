#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{
    EraseNoteProjector, boot_erase_engine, count_ledger_by_author, count_memo_facts, count_memos,
    count_notes, published_note, seed_ledger, seed_memo, seed_note,
};
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, removed_key, reset_views, window,
};
use engine_twin::{SOON, await_ready};
use service_engine::delta::Delta;
use service_engine::{OutboxRelay, PersonId};
use sqlx::PgPool;
use uuid::Uuid;

const SERVICE: &str = "erasesample";

fn reset_note_ids(delta: &Delta) -> Vec<Uuid> {
    reset_views(delta)
        .iter()
        .map(|view| view.key.decode::<Uuid>().expect("a note key decodes"))
        .collect()
}

async fn seed_chunk(pool: &PgPool, note: Uuid) {
    sqlx::query(
        "INSERT INTO service_engine.accumulator_chunk (accumulator, key, seq, chunk) \
         VALUES ('erase_note_stream', $1, 0, $2)",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .bind(serde_json::json!("chunk-0"))
    .execute(pool)
    .await
    .expect("seed an accumulated-lane chunk for the person's note");
}

async fn chunk_count(pool: &PgPool, note: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.accumulator_chunk \
         WHERE accumulator = 'erase_note_stream' AND key = $1",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .fetch_one(pool)
    .await
    .expect("count the person's stream chunks")
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
async fn s70_erase_removes_state_across_crud_soft_full_in_one_gesture_and_is_idempotent() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let other = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let mine = Uuid::now_v7();
    let theirs = Uuid::now_v7();
    seed_note(&pool, mine, person, tenant, "mine").await;
    seed_note(&pool, theirs, other, tenant, "theirs").await;
    seed_chunk(&pool, mine).await;
    let memo = Uuid::now_v7();
    seed_memo(&pool, memo, person, tenant, "soft body").await;
    let ledger = Uuid::now_v7();
    seed_ledger(&pool, ledger, tenant, person, 7).await;

    let engine = boot_erase_engine(&db, fabric.clone(), "se_s65", "pod-s65", SERVICE).await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let eraser = engine.eraser();
    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(EraseNoteProjector::NAME, false)],
        ))
        .await
        .expect("the session attaches to the note projector");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let mut ids = reset_note_ids(&reset);
    ids.sort();
    let mut expected = vec![mine, theirs];
    expected.sort();
    assert_eq!(ids, expected, "the snapshot holds both persons' notes");

    let offered = await_note_offer(&fabric, mine, true).await;
    assert!(
        offered,
        "the leader reconcile published the person's note offer"
    );

    let outcome = eraser
        .erase(PersonId(person))
        .await
        .expect("the erase gesture runs every registered slice in one transaction");
    assert!(outcome.fresh, "the first erase records the erasure fact");
    assert_eq!(
        outcome.rows_erased, 3,
        "one CRUD note, one soft memo and one full ledger were erased in one tx"
    );

    assert_eq!(count_notes(&pool, person).await, 0, "the CRUD note is gone");
    assert_eq!(
        count_notes(&pool, other).await,
        1,
        "another person is untouched"
    );
    assert_eq!(
        count_memos(&pool, person).await,
        0,
        "the soft memo row is gone"
    );
    assert_eq!(
        count_memo_facts(&pool, person).await,
        0,
        "the soft fact log carrying the person's value is scrubbed"
    );
    assert_eq!(
        count_ledger_by_author(&pool, person).await,
        0,
        "the full-EDA events and snapshot no longer name the person"
    );
    assert_eq!(
        chunk_count(&pool, mine).await,
        0,
        "the accumulated-lane stream subjects of the person are purged after commit"
    );

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the erased note removes from the live session");
    assert!(
        matches!(delta, Delta::Remove { .. }),
        "the person's view disappears with a Remove, got {delta:?}"
    );
    assert_eq!(
        removed_key(&delta),
        mine,
        "only the erased person's note is removed"
    );

    let retracted = await_note_offer(&fabric, mine, false).await;
    assert!(
        !retracted,
        "the leader retracts the offer for the erased note"
    );

    assert_eq!(
        person_erased_rows(&pool).await,
        1,
        "PersonErased is staged in the outbox exactly once"
    );
    OutboxRelay::new(pool.clone(), fabric.clone())
        .run_once_detailed()
        .await
        .expect("the leader relay publishes the staged PersonErased");

    let repeat = eraser
        .erase(PersonId(person))
        .await
        .expect("erasing an already-erased person succeeds");
    assert!(
        !repeat.fresh,
        "the second erase finds the fact already recorded"
    );
    assert_eq!(repeat.rows_erased, 0, "the second erase touches nothing");
    assert_eq!(
        person_erased_rows(&pool).await,
        1,
        "the second erase does not stage PersonErased again"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_note_offer(fabric: &service_engine::Nats, id: Uuid, want_present: bool) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let present = published_note(fabric, id).await.is_some();
        if present == want_present {
            return present;
        }
        if tokio::time::Instant::now() >= deadline {
            return present;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
