mod blackbox_support;

use std::time::Duration;

use blackbox_support::bin::{SpawnEnv, free_port, read_log, spawn_subcommand, wait_exit};
use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::infra::TestNats;
use service_engine::engine::boot::REASON_MIGRATIONS_PENDING;

#[tokio::test]
async fn bb12_serve_refuses_an_unmigrated_store() {
    let db = BlackboxDb::fresh().await;
    let nats = TestNats::spawn().await;

    let env = SpawnEnv {
        database_url: db.app_url.clone(),
        owner_database_url: db.owner_url.clone(),
        app_role: db.app_role.clone(),
        nats_url: nats.url(),
        pod: "bb12-pod".to_string(),
        port: free_port(),
        blobs: None,
        session_bounds: None,
        mirror: None,
    };

    let (mut child, log) = spawn_subcommand(&env, "serve", "bb12");
    let status = wait_exit(&mut child, Duration::from_secs(30))
        .await
        .expect("serve exits instead of serving an unmigrated store");

    assert!(
        !status.success(),
        "serve exits non-zero on an unmigrated store, got {status}"
    );
    let log = read_log(&log);
    assert!(
        log.contains(REASON_MIGRATIONS_PENDING),
        "the refusal names REASON_MIGRATIONS_PENDING, got:\n{log}"
    );

    drop(nats);
    db.cleanup().await;
}
