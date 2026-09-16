mod blackbox_support;

use std::process::Command;

use blackbox_support::World;
use blackbox_support::bin::example_service_bin;
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};

/// The composed SDL, built in-process the same way the boot kit builds it: from the
/// service's own GraphQL roots, with no engine state attached (state never changes the
/// type graph). This is the oracle both `/sdl` and the `schema` subcommand must match.
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
    // Starting the binary against a database the harness only *provisioned* (roles +
    // CONNECT, nothing migrated) proves the kit ran the owner→migrate→grant sequence
    // before serving: it could not answer /readyz otherwise.
    let world = World::start("bb07-pod").await;
    let http = reqwest::Client::new();

    // /livez is always 200 "alive" — liveness never gates on a dependency.
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

    // /metrics exposes the process-global Prometheus recorder the kit installed; the
    // universal process collectors are always present in the exposition.
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

    // /sdl serves the composed schema, byte-for-byte the service's own SDL.
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

    // The `schema` subcommand prints the same SDL and exits, touching no infra.
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
