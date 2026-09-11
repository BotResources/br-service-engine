use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::housekeeping::scheduled_message::fire_due;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use sqlx::PgPool;
use uuid::Uuid;

const POISON_SUBJECT: &str = "nostream.s159.poison";
const GOOD_SUBJECT: &str = "integration.cmd.s159.work.do.v1";

async fn schedule(pool: &PgPool, subject: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO service_engine.scheduled_message \
           (id, at, source, subject, message_id, payload) \
         VALUES ($1, now(), 'test', $2, $3, $4)",
    )
    .bind(id)
    .bind(subject)
    .bind(Uuid::now_v7())
    .bind(b"{}".as_slice())
    .execute(pool)
    .await
    .expect("stage a scheduled message");
    id
}

async fn pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.scheduled_message")
        .fetch_one(pool)
        .await
        .expect("count the scheduled rows")
}

#[tokio::test]
async fn s159_a_poison_scheduled_row_dead_letters_and_the_next_row_still_fires() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let engine_nats = nats.nats().await;
    let pool = db.app_pool().clone();
    let dead_letters = DeadLetters::new(pool.clone());

    schedule(&pool, POISON_SUBJECT).await;
    schedule(&pool, GOOD_SUBJECT).await;

    let round = fire_due(&pool, &engine_nats, &dead_letters, 128, 1)
        .await
        .expect("the beat fires the due scheduled messages");
    assert_eq!(
        round.fired, 1,
        "the good message fires even though a poison row sat in the same batch"
    );
    assert_eq!(
        round.dead_lettered, 1,
        "the poison row exhausts its one-attempt budget and is dead-lettered rather than \
         re-claimed at every beat forever"
    );
    assert_eq!(
        pending(&pool).await,
        0,
        "both rows leave the queue: the good one because it fired, the poison one because it was \
         dead-lettered"
    );

    let dead = dead_letters
        .list(Some(DeadLetterSource::Scheduled), 16)
        .await
        .expect("read the dead-letter table");
    assert_eq!(dead.len(), 1, "exactly the poison row is recorded");
    assert_eq!(dead[0].subject, POISON_SUBJECT);

    schedule(&pool, GOOD_SUBJECT).await;
    let next = fire_due(&pool, &engine_nats, &dead_letters, 128, 1)
        .await
        .expect("a later beat keeps firing");
    assert_eq!(
        next.fired, 1,
        "the queue keeps flowing on the following beat; the earlier poison never blocked it"
    );

    drop(nats);
    db.cleanup().await;
}
