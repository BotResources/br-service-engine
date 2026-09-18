use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::stage_outbox_row;
use service_engine::relays::outbox::OutboxRelay;
use sqlx::PgPool;
use uuid::Uuid;

const RETENTION: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn s155_the_beat_sweeps_published_rows_and_stale_claims_but_keeps_recent_ones() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let mut ids = Vec::new();
    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..3 {
        ids.push(stage_outbox_row(&mut tx, &format!("published-{n}")).await);
    }
    tx.commit().await.expect("commit the staged rows");

    let relay = OutboxRelay::new(pool.clone(), fabric.clone());
    relay
        .run_once_detailed()
        .await
        .expect("the drain publishes the staged rows and marks them PUBLISHED");
    assert_eq!(
        published_rows(&pool).await,
        3,
        "the drain leaves three PUBLISHED rows behind"
    );

    backdate_published(&pool, &[ids[0], ids[1]]).await;
    insert_claim(&pool, "reaction.old", Duration::from_secs(2 * 3600)).await;
    insert_claim(&pool, "reaction.also_old", Duration::from_secs(2 * 3600)).await;
    insert_claim(&pool, "reaction.fresh", Duration::ZERO).await;

    let swept = relay
        .sweep(RETENTION)
        .await
        .expect("the hygiene sweep runs against the store");

    assert_eq!(
        swept.published, 2,
        "only the two rows published past the retention window are deleted"
    );
    assert_eq!(
        swept.claims, 2,
        "only the two claims older than the retention window are deleted"
    );
    assert_eq!(
        published_rows(&pool).await,
        1,
        "the freshly published row is kept until it too ages past the retention"
    );
    assert_eq!(
        claim_rows(&pool).await,
        1,
        "the fresh claim, still inside the dedup window, is kept"
    );

    db.cleanup().await;
}

async fn published_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.integration_outbox WHERE status = 'PUBLISHED'",
    )
    .fetch_one(pool)
    .await
    .expect("count the published rows")
}

async fn claim_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.message_claim")
        .fetch_one(pool)
        .await
        .expect("count the message claims")
}

async fn backdate_published(pool: &PgPool, ids: &[Uuid]) {
    sqlx::query(
        "UPDATE service_engine.integration_outbox \
         SET published_at = now() - make_interval(secs => $2) \
         WHERE id = ANY($1)",
    )
    .bind(ids)
    .bind((2 * 3600) as f64)
    .execute(pool)
    .await
    .expect("age the published rows past the retention window");
}

async fn insert_claim(pool: &PgPool, reaction: &str, age: Duration) {
    sqlx::query(
        "INSERT INTO service_engine.message_claim (message_id, reaction, claimed_at) \
         VALUES ($1, $2, now() - make_interval(secs => $3))",
    )
    .bind(Uuid::now_v7())
    .bind(reaction)
    .bind(age.as_secs_f64())
    .execute(pool)
    .await
    .expect("insert a message claim at a chosen age");
}
