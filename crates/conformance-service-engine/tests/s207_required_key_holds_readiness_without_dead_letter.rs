use std::sync::Arc;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    REQUIRED_USER_KEY, RecordingTransport, mirror_dead_letters, publish_required_user,
    required_key_mirror, retract_required_user,
};

#[tokio::test]
async fn s207_a_required_key_gates_readiness_and_never_dead_letters() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let mirror =
        required_key_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    let required = mirror
        .required_keys()
        .expect("a mirror that declares a required key exposes its readiness channel");

    mirror
        .reconcile()
        .await
        .expect("an absent required key never fails the reconcile");
    assert!(
        required
            .borrow()
            .iter()
            .any(|entry| entry.contains(REQUIRED_USER_KEY)),
        "an absent required key holds the pod out of rotation, naming the key"
    );
    assert!(
        mirror_dead_letters(&pool).await.is_empty(),
        "a required key is configuration, never a dead letter"
    );

    publish_required_user(&fabric, "config@example.test").await;
    mirror
        .reconcile()
        .await
        .expect("the reconcile absorbs the required key");
    assert!(
        required.borrow().is_empty(),
        "once the key exists the pod is ready"
    );

    retract_required_user(&fabric).await;
    mirror
        .reconcile()
        .await
        .expect("the reconcile absorbs the retraction");
    assert!(
        required
            .borrow()
            .iter()
            .any(|entry| entry.contains(REQUIRED_USER_KEY)),
        "deleting the key holds the pod out of rotation again"
    );
    assert!(
        mirror_dead_letters(&pool).await.is_empty(),
        "the mirror stays converged and dead-letters nothing throughout"
    );

    drop(nats);
    db.cleanup().await;
}
