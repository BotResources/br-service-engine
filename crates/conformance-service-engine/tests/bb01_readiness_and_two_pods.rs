mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::World;

#[tokio::test]
async fn bb01_the_binary_declares_scopes_reaches_readiness_and_a_second_pod_shares_the_store() {
    let world = World::start("bb01-pod-a").await;

    let mut seen = world.scope_declarations_seen();
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        seen = world.scope_declarations_seen();
    }
    assert!(
        seen >= 1,
        "the running binary declared its scopes to identity over NATS at boot"
    );

    let http = reqwest::Client::new();
    let ready = http
        .get(format!("{}/readyz", world.base_url()))
        .send()
        .await
        .expect("the binary answers /readyz")
        .status();
    assert_eq!(
        ready.as_u16(),
        200,
        "the first pod is UP once its infra is bound and its scopes accepted"
    );

    let pod_b = world.spawn_pod("bb01-pod-b").await;
    let ready_b = http
        .get(format!("{}/readyz", pod_b.base_url()))
        .send()
        .await
        .expect("the second pod answers /readyz")
        .status();
    assert_eq!(
        ready_b.as_u16(),
        200,
        "a second pod boots against the same Postgres and NATS and reaches readiness"
    );

    pod_b.shutdown().await;
    world.cleanup().await;
}
