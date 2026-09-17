mod blackbox_support;

use std::process::Command;

use blackbox_support::World;
use blackbox_support::bin::example_service_bin;
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};

fn expected_sdl() -> String {
    async_graphql::Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish()
    .sdl()
}

#[tokio::test]
async fn bb07_the_boot_kit_serves_livez_metrics_and_sdl_and_a_subcommand_prints_the_schema() {
    let world = World::start("bb07-pod").await;
    let http = reqwest::Client::new();
    let livez = http
        .get(format!("{}/livez", world.base_url()))
        .send()
        .await
        .expect("the binary answers /livez");
    assert_eq!(livez.status().as_u16(), 200, "livez is 200");
    assert_eq!(
        livez.text().await.expect("livez body"),
        "alive",
        "livez body is the sentinel"
    );
    let metrics = http
        .get(format!("{}/metrics", world.base_url()))
        .send()
        .await
        .expect("the binary answers /metrics");
    assert_eq!(metrics.status().as_u16(), 200, "metrics is 200");
    let content_type = metrics
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/plain"),
        "metrics is Prometheus text exposition, got {content_type}"
    );
    let metrics_body = metrics.text().await.expect("metrics body");
    assert!(
        metrics_body.contains("process_"),
        "the process collectors are exported: {metrics_body}"
    );
    let expected = expected_sdl();
    let sdl = http
        .get(format!("{}/sdl", world.base_url()))
        .send()
        .await
        .expect("the binary answers /sdl");
    assert_eq!(sdl.status().as_u16(), 200, "sdl is 200");
    let sdl_body = sdl.text().await.expect("sdl body");
    assert_eq!(
        sdl_body, expected,
        "the /sdl route serves exactly the composed schema"
    );
    let output = Command::new(example_service_bin())
        .arg("schema")
        .output()
        .expect("run the example-service `schema` subcommand");
    assert!(
        output.status.success(),
        "the schema subcommand exits 0: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed = String::from_utf8(output.stdout).expect("the subcommand prints UTF-8 SDL");
    assert_eq!(
        printed.trim_end(),
        expected.trim_end(),
        "the schema subcommand prints exactly the composed schema"
    );

    world.cleanup().await;
}
