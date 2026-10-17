use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    DIRECTORY_MIRROR, RecordingTransport, SampleDirectory, directory_mirror,
    directory_mirror_handle, known_users, mirror_dead_letters, publish_roster,
    publish_versioned_user, user_prefix,
};
use service_engine::housekeeping::mirror::{MirrorCondition, MirrorSupervisor};
use service_engine::stop::Stop;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const CONFIDENTIAL: &str = "confidential-token-7731";

#[tokio::test]
async fn s280_an_undecodable_consumed_value_is_dead_lettered_alone_and_the_mirror_stays_converged()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(first, "one@example.test"), (second, "two@example.test")]),
    )
    .await;
    let not_json = Uuid::now_v7();
    let wrong_shape = Uuid::now_v7();
    nats.put_raw(
        &user_key(not_json),
        format!("not json {CONFIDENTIAL}").as_bytes(),
    )
    .await;
    nats.put_raw(
        &user_key(wrong_shape),
        format!("{{\"email\": [\"{CONFIDENTIAL}\"]}}").as_bytes(),
    )
    .await;

    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(directory_mirror_handle(
            fabric.clone(),
            pool.clone(),
            Arc::new(RecordingTransport),
        ))
        .expect("the directory mirror registers");
    let board = supervisor.health();
    let shutdown = Stop::new();
    let mut tasks = supervisor.start(shutdown.clone());
    assert!(
        tokio::time::timeout(OBSERVED_WITHIN, tasks.converged())
            .await
            .expect("the mirror converged before the deadline"),
        "undecodable values at boot never keep the mirror from converging"
    );
    assert_eq!(
        known_users(&pool).await,
        2,
        "the two good values project; the two undecodable ones do not"
    );
    assert_eq!(
        dead_letter_subjects(&pool).await,
        sorted(&[user_key(not_json), user_key(wrong_shape)]),
        "one dead letter per undecodable key, none for the good ones"
    );

    let late_good = Uuid::now_v7();
    let late_bad = Uuid::now_v7();
    nats.put_raw(&user_key(late_bad), b"{{{ truncated").await;
    publish_versioned_user(&fabric, late_good, "late@example.test", 1).await;
    await_users(&pool, 3).await;
    assert_eq!(
        dead_letter_subjects(&pool).await,
        sorted(&[
            user_key(not_json),
            user_key(wrong_shape),
            user_key(late_bad)
        ]),
        "a value that turns undecodable while the mirror watches is dead-lettered too"
    );

    nats.put_raw(&user_key(first), b"garbage over a good value")
        .await;
    await_users(&pool, 2).await;
    assert_eq!(
        dead_letter_subjects(&pool).await.len(),
        4,
        "a good value overwritten by garbage is dead-lettered and retired from the projection"
    );

    let before = last_seen(&pool, &user_key(not_json)).await;
    nats.put_raw(&user_key(not_json), b"the same bad key, written again")
        .await;
    await_row_seen_again(&pool, &user_key(not_json), before).await;
    assert_eq!(
        dead_letter_subjects(&pool).await.len(),
        4,
        "a bad key written again updates its row; it never adds one"
    );

    assert_eq!(tasks.restarts(), 0, "no value ever restarted the mirror");
    assert_eq!(
        board.borrow().condition(&DIRECTORY_MIRROR),
        Some(&MirrorCondition::Converged),
        "the mirror is still converged"
    );
    let stuck: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.dead_letter WHERE source = 'mirror' AND reaction = subject",
    )
    .fetch_one(&pool)
    .await
    .expect("count stuck-mirror dead letters");
    assert_eq!(stuck, 0, "no stuck-mirror dead letter was written");

    shutdown.stop();

    let rows_before = dead_letter_subjects(&pool).await.len();
    let second_read =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    second_read
        .reconcile()
        .await
        .expect("a full read over undecodable values succeeds");
    second_read
        .reconcile()
        .await
        .expect("and so does a repeat of it");
    assert_eq!(
        dead_letter_subjects(&pool).await.len(),
        rows_before,
        "re-reading the same bad values multiplies no row"
    );
    let errors: Vec<String> = sqlx::query_scalar(
        "SELECT error FROM service_engine.dead_letter WHERE source = 'mirror' ORDER BY subject",
    )
    .fetch_all(&pool)
    .await
    .expect("read the dead-letter reasons");
    assert!(
        errors
            .iter()
            .all(|error| error.starts_with("undecodable_value") && !error.contains(CONFIDENTIAL)),
        "every reason carries the stable code and never the value, got {errors:?}"
    );

    drop(nats);
    db.cleanup().await;
}

fn user_key(id: Uuid) -> String {
    format!("{}{id}", user_prefix())
}

fn sorted(keys: &[String]) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = keys
        .iter()
        .map(|key| (DIRECTORY_MIRROR.as_str().to_string(), key.clone()))
        .collect();
    rows.sort_by(|a, b| a.1.cmp(&b.1));
    rows
}

async fn dead_letter_subjects(pool: &PgPool) -> Vec<(String, String)> {
    mirror_dead_letters(pool).await
}

async fn await_users(pool: &PgPool, want: i64) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if known_users(pool).await == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "known_users never reached {want}"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn last_seen(pool: &PgPool, key: &str) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar(
        "SELECT last_seen FROM service_engine.dead_letter WHERE source = 'mirror' AND subject = $1",
    )
    .bind(key)
    .fetch_one(pool)
    .await
    .expect("read when the dead letter was last seen")
}

async fn await_row_seen_again(pool: &PgPool, key: &str, before: chrono::DateTime<chrono::Utc>) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if last_seen(pool, key).await > before {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the mirror never processed the rewritten bad value"
        );
        tokio::time::sleep(POLL).await;
    }
}
