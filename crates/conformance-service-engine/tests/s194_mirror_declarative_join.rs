//! The declarative `KnownRow` kit projects a typed two-offer join into two
//! `known_*` tables from an empty start, with no hand-written SQL in the
//! projector. It also retires a row the offer no longer carries — proving the
//! generated `DELETE` and the generated `INSERT … ON CONFLICT` both land.

use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, SampleDirectory, declarative_group_name, declarative_mirror,
    declarative_user_email, publish_group, publish_roster,
};
use uuid::Uuid;

#[tokio::test]
async fn s194_the_declarative_kit_joins_two_offers_into_two_known_tables_from_empty() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    // A row a wider earlier run left behind, to prove the generated DELETE
    // retires what the offers no longer carry.
    let stale_user = Uuid::now_v7();
    sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
        .bind(stale_user)
        .bind("stale@example.test")
        .execute(&pool)
        .await
        .expect("a user mirrored by an earlier run");

    let user = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "one@example.test")]),
    )
    .await;
    let group = Uuid::now_v7();
    publish_group(&fabric, group, "team", &[user]).await;

    let mirror =
        declarative_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("the boot join reads both offers and projects both known tables");

    assert_eq!(
        declarative_user_email(&pool, user).await.as_deref(),
        Some("one@example.test"),
        "the generated upsert projected the user offer into known_users"
    );
    assert_eq!(
        declarative_group_name(&pool, group).await.as_deref(),
        Some("team"),
        "the generated upsert projected the group offer into known_groups"
    );
    assert_eq!(
        declarative_user_email(&pool, stale_user).await,
        None,
        "the generated delete retired the user the offer no longer carries"
    );

    drop(nats);
    db.cleanup().await;
}
