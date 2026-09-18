use std::sync::Arc;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    RecordingTransport, directory_mirror, known_users, mirror_dead_letters, publish_versioned_user,
    user_prefix,
};
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn s208_a_value_at_the_wrong_wire_version_is_dead_lettered_alone() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    let stale = Uuid::now_v7();
    publish_versioned_user(&fabric, first, "one@example.test", 1).await;
    publish_versioned_user(&fabric, second, "two@example.test", 1).await;
    publish_versioned_user(&fabric, stale, "stale@example.test", 2).await;

    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("a per-value wire mismatch never fails the reconcile");

    assert_eq!(
        known_users(&pool).await,
        2,
        "the two values at the coded version project; the mismatched one does not"
    );
    assert_eq!(
        user_email(&pool, stale).await,
        None,
        "the value at the wrong wire version never entered known_users"
    );
    assert_eq!(
        mirror_dead_letters(&pool).await,
        vec![("directory".to_string(), format!("{}{stale}", user_prefix()))],
        "one dead-letter row keyed on (mirror, key)"
    );

    drop(nats);
    db.cleanup().await;
}

async fn user_email(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT email FROM known_users WHERE user_id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read a mirrored user")
}
