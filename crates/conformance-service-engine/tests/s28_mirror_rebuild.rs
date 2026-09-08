use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, SampleDirectory, directory_mirror, publish_roster, staged_impacts,
};
use service_engine::impact::{ForeignKey, Impact};
use uuid::Uuid;

#[tokio::test]
async fn s28_boot_retires_a_known_row_the_bucket_no_longer_offers() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let kept = Uuid::now_v7();
    let retired = Uuid::now_v7();
    for (id, email) in [(kept, "kept@example.test"), (retired, "gone@example.test")] {
        sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .expect("a mirrored row surviving from an earlier, wider run");
    }

    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(kept, "kept@example.test")]),
    )
    .await;

    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("the boot full-read rebuilds known_users from the bucket");

    let present: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users ORDER BY user_id")
        .fetch_all(&pool)
        .await
        .expect("read the rebuilt roster");
    assert_eq!(
        present,
        vec![kept],
        "the rebuild keeps the offered user and retires the one the bucket dropped while the pod was down"
    );

    let staged = staged_impacts(&pool).await;
    let retired_impact =
        Impact::foreign(ForeignKey::new("identity.user", &retired.to_string()).unwrap());
    assert!(
        staged.contains(&retired_impact),
        "retiring a stale row stages a foreign impact so live sessions drop it, not just the store"
    );

    drop(nats);
    db.cleanup().await;
}
