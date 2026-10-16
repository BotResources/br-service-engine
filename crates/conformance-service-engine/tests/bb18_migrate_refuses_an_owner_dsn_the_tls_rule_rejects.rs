mod blackbox_support;

use std::time::Duration;

use blackbox_support::bin::{SpawnEnv, free_port, read_log, spawn_migrate_owner_only, wait_exit};

const REFUSED: [(&str, &str); 2] = [
    (
        "postgres://db.remote.example/app?options=@127.0.0.1",
        "requires TLS",
    ),
    ("postgres:///db?sslmode=require", "no host"),
];

#[tokio::test]
async fn bb18_migrate_refuses_an_owner_dsn_the_tls_rule_rejects() {
    for (index, (owner_url, reason)) in REFUSED.into_iter().enumerate() {
        let env = SpawnEnv {
            database_url: owner_url.to_string(),
            owner_database_url: owner_url.to_string(),
            app_role: "bb18_app".to_string(),
            nats_url: "nats://127.0.0.1:1".to_string(),
            pod: format!("bb18-pod-{index}"),
            port: free_port(),
            blobs: None,
            session_bounds: None,
            mirror: None,
        };

        let (mut child, log) = spawn_migrate_owner_only(&env, "bb18");
        let status = wait_exit(&mut child, Duration::from_secs(30))
            .await
            .unwrap_or_else(|| {
                let _ = child.kill();
                panic!(
                    "migrate must refuse {owner_url} before any connection, it was still \
                     retrying after 30s:\n{}",
                    read_log(&log)
                )
            });
        let output = read_log(&log);
        assert!(
            !status.success(),
            "migrate refuses {owner_url} and exits non-zero, got {status}:\n{output}"
        );
        assert!(
            output.contains(reason),
            "the refusal of {owner_url} states `{reason}`:\n{output}"
        );
        assert!(
            !output.contains("owner connection not ready"),
            "the refusal of {owner_url} lands before any connection attempt:\n{output}"
        );
    }
}
