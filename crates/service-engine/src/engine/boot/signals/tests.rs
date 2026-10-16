use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::ProcessSignals;

const WITHIN: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(200);

fn send_to_self(signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(std::process::id().to_string())
        .status()
        .expect("kill is available");
    assert!(status.success());
}

async fn assert_one_shutdown_request(shutdown: &Notify) {
    tokio::time::timeout(WITHIN, shutdown.notified())
        .await
        .expect("the signal triggers the engine shutdown handle");
    assert!(
        tokio::time::timeout(QUIET, shutdown.notified())
            .await
            .is_err(),
        "one signal stores one shutdown permit, never a second"
    );
}

#[tokio::test]
async fn a_sigterm_triggers_the_engine_shutdown_handle() {
    let shutdown = Arc::new(Notify::new());
    ProcessSignals::install()
        .expect("the signal handlers install")
        .request_shutdown_on_signal(shutdown.clone());
    send_to_self("TERM");
    assert_one_shutdown_request(&shutdown).await;
}

#[tokio::test]
async fn a_sigint_triggers_the_engine_shutdown_handle() {
    let shutdown = Arc::new(Notify::new());
    ProcessSignals::install()
        .expect("the signal handlers install")
        .request_shutdown_on_signal(shutdown.clone());
    send_to_self("INT");
    assert_one_shutdown_request(&shutdown).await;
}

#[tokio::test]
async fn a_service_task_notifying_beside_the_signal_task_still_requests_one_shutdown() {
    let shutdown = Arc::new(Notify::new());
    ProcessSignals::install()
        .expect("the signal handlers install")
        .request_shutdown_on_signal(shutdown.clone());
    shutdown.notify_one();
    send_to_self("TERM");
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.notify_one();
    assert_one_shutdown_request(&shutdown).await;
}
