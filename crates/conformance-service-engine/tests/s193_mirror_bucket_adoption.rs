mod mirror_support;

use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use mirror_support::{
    OTHER_LANGUAGE, OtherEntry, PUBLISHED_LANGUAGE, await_count, catalog, count, email_of, failing,
    joined, last_sequence, publish, seed, watermark,
};
use service_engine::nats::KvKey;
use uuid::Uuid;

#[tokio::test]
async fn s193_a_replaced_bucket_is_adopted_rather_than_refused() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let first = Uuid::now_v7();
    publish(&fabric, "catalog/one", first, "old").await;
    catalog("replaced_catalog")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the first boot projects what the bucket holds");
    let before = watermark(&pool, "replaced_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("the first boot committed an identity");

    fabric
        .context()
        .delete_key_value(PUBLISHED_LANGUAGE)
        .await
        .expect("an operator drops the bucket");
    fabric
        .context()
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: PUBLISHED_LANGUAGE.into(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("and recreates it");
    let second = Uuid::now_v7();
    for round in 0..4 {
        publish(&fabric, "catalog/two", second, &format!("new-{round}")).await;
    }

    catalog("replaced_catalog")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("a replaced bucket is adopted: a full read, a reconcile, and the new identity");

    let after = watermark(&pool, "replaced_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("the adoption committed the new identity");
    assert_ne!(after.1, before.1, "the mirror adopted the new stream");
    assert_eq!(
        after.0,
        last_sequence(&fabric, PUBLISHED_LANGUAGE).await as i64
    );
    assert_eq!(
        email_of(&pool, second).await.as_deref(),
        Some("new-3"),
        "the replacement's content is projected"
    );
    assert_eq!(
        email_of(&pool, first).await,
        None,
        "and the row the replaced bucket no longer carries is reconciled away"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s193_a_sequence_below_the_held_boundary_is_adopted_rather_than_called_a_rollback() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let id = Uuid::now_v7();
    publish(&fabric, "catalog/one", id, "current").await;
    catalog("restored_catalog")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the first boot converges");

    sqlx::query(
        "UPDATE service_engine.mirror_watermark SET revision = revision + 100 \
         WHERE mirror = 'restored_catalog'",
    )
    .execute(&pool)
    .await
    .expect("the held boundary runs ahead of the broker");

    catalog("restored_catalog")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("a boundary the broker is behind is adopted, not refused");

    assert_eq!(
        watermark(&pool, "restored_catalog", PUBLISHED_LANGUAGE)
            .await
            .expect("the adoption committed the boundary it read")
            .0,
        last_sequence(&fabric, PUBLISHED_LANGUAGE).await as i64,
        "the read adopts the sequence the bucket actually has, ahead or behind"
    );
    assert_eq!(email_of(&pool, id).await.as_deref(), Some("current"));

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s193_a_row_with_no_adopted_identity_takes_the_one_the_read_finds() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    seed(&pool, "from a release before the identity column").await;
    sqlx::query(
        "INSERT INTO service_engine.mirror_watermark (mirror, bucket, revision) \
         VALUES ('legacy_catalog', 'PUBLISHED_LANGUAGE', 123)",
    )
    .execute(&pool)
    .await
    .expect("an upgraded installation carries a watermark with no identity");

    catalog("legacy_catalog")
        .build(
            nats.nats().await,
            pool.clone(),
            Arc::new(RecordingTransport),
        )
        .reconcile()
        .await
        .expect("the first read after the upgrade adopts the identity it finds");

    let held = watermark(&pool, "legacy_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("the row is still there");
    assert_eq!(
        held.0, 0,
        "adoption replaces the untrusted cursor with the boundary it read"
    );
    assert!(held.1.is_some());
    assert_eq!(
        count(&pool).await,
        0,
        "and the bucket it read is what known_* follows"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s193_a_failed_reconcile_commits_neither_the_projection_nor_the_boundary() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    seed(&pool, "preserved").await;

    failing("failing_catalog")
        .build(
            nats.nats().await,
            pool.clone(),
            Arc::new(RecordingTransport),
        )
        .reconcile()
        .await
        .expect_err("the projector fails after it has written");

    assert_eq!(
        count(&pool).await,
        1,
        "the projection and the boundary share one transaction, so a failure rolls both back"
    );
    assert!(
        watermark(&pool, "failing_catalog", PUBLISHED_LANGUAGE)
            .await
            .is_none(),
        "a read that did not complete adopts nothing"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s193_identities_and_boundaries_are_per_bucket_even_under_one_prefix() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();
    fabric
        .context()
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: OTHER_LANGUAGE.into(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("a second bucket carrying the same prefix");

    let here = Uuid::now_v7();
    publish(&fabric, "catalog/one", here, "one").await;
    let there = Uuid::now_v7();
    let key = KvKey::new("catalog/two").unwrap();
    let other = fabric
        .bind_kv::<OtherEntry>(OTHER_LANGUAGE)
        .await
        .expect("bind the second bucket");
    for round in 0..5 {
        other
            .put(
                &key,
                &OtherEntry {
                    id: there,
                    value: format!("two-{round}"),
                },
            )
            .await
            .expect("the second bucket runs its own sequence");
    }

    let mirror =
        joined("joined_catalog").build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror.reconcile().await.expect("both buckets read");
    assert_eq!(count(&pool).await, 2);

    let one = watermark(&pool, "joined_catalog", PUBLISHED_LANGUAGE)
        .await
        .expect("the first bucket's boundary");
    let two = watermark(&pool, "joined_catalog", OTHER_LANGUAGE)
        .await
        .expect("the second bucket's boundary");
    assert_eq!(one.0, 1);
    assert_eq!(two.0, 5);
    assert_ne!(one.1, two.1, "two streams, two identities");

    let watching = tokio::spawn(mirror.watch());
    other.retract(&key).await.expect("retract in one bucket");
    await_count(
        &pool,
        1,
        "the retract reaches known_* through its own bucket",
    )
    .await;
    assert_eq!(
        watermark(&pool, "joined_catalog", PUBLISHED_LANGUAGE).await,
        Some(one),
        "and the other bucket's boundary is untouched by it"
    );

    watching.abort();
    drop(nats);
    db.cleanup().await;
}
