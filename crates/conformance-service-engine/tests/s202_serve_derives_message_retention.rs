mod blackbox_support;

use std::time::Duration;

use blackbox_support::World;

#[tokio::test]
async fn s202_serve_derives_message_retention_from_the_bound_streams() {
    let two_hours = Duration::from_secs(2 * 60 * 60);
    let world = World::start_with_integration_max_age("s202-pod", two_hours).await;

    let http = reqwest::Client::new();
    let readyz = http
        .get(format!("{}/readyz", world.base_url()))
        .send()
        .await
        .expect("the service answers /readyz");
    assert_eq!(
        readyz.status().as_u16(),
        200,
        "the service boots ready with no service-side retention because serve derives it from \
         the bound streams' max_age; the default 1 h would not cover a 2 h stream"
    );

    world.cleanup().await;
}
