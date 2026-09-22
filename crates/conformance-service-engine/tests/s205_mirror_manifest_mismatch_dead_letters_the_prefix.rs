use std::sync::Arc;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    RecordingTransport, SampleDirectory, directory_mirror, known_users, mirror_dead_letters,
    publish_offer_manifest, publish_roster, user_prefix,
};
use uuid::Uuid;

#[tokio::test]
async fn s205_a_manifest_at_the_wrong_version_withholds_the_whole_prefix() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let one = Uuid::now_v7();
    let two = Uuid::now_v7();
    for (id, email) in [(one, "one@example.test"), (two, "two@example.test")] {
        sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .expect("a user a prior run mirrored");
    }
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(one, "one@example.test"), (two, "two@example.test")]),
    )
    .await;

    publish_offer_manifest(&fabric, user_prefix(), 2).await;

    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("a manifest mismatch never fails the reconcile");

    assert_eq!(
        known_users(&pool).await,
        0,
        "the prefix is rejected wholesale, so known_users follows to empty"
    );
    assert_eq!(
        mirror_dead_letters(&pool).await,
        vec![("directory".to_string(), user_prefix().to_string())],
        "one dead-letter row keyed on (mirror, prefix)"
    );

    drop(nats);
    db.cleanup().await;
}
