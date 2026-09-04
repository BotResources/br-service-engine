use super::*;
use crate::housekeeping::mirror::tests::{Probe, flapping, until};
use crate::mirror::MirrorRun;
use std::time::Duration;

fn panics_once_then_flaps(probe: Arc<Probe>) -> MirrorHandle {
    MirrorHandle::new(
        MirrorName::from_static("directory"),
        {
            let probe = probe.clone();
            move || {
                let probe = probe.clone();
                Box::pin(async move {
                    probe.reconciles.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }) as MirrorRun
            }
        },
        {
            let probe = probe.clone();
            move || {
                let probe = probe.clone();
                Box::pin(async move {
                    let seen = probe.watches.fetch_add(1, Ordering::SeqCst);
                    if seen == 0 {
                        panic!("the roster watch panicked deep in a KV frame");
                    }
                    Err(EngineError::Service("the roster stream ended".into()))
                }) as MirrorRun
            }
        },
    )
}

#[tokio::test]
async fn a_watch_that_panics_is_restarted_exactly_like_a_watch_that_errored() {
    let probe = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(panics_once_then_flaps(probe.clone()))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let tasks = supervisor.start(shutdown.clone());

    assert!(
        until(Duration::from_secs(10), || Probe::count(&probe.watches)
            >= 2)
        .await,
        "a mirror whose watch panicked must be reconciled and watched again; before the fix the \
         panic killed the supervisor task and the watch was never retried"
    );
    shutdown.notify_waiters();
    assert!(
        tasks.restarts() >= 1,
        "the panic took the same restart path as an Err, so the mirror self-heals"
    );
    assert!(
        !tasks.is_converged(),
        "a mirror that just panicked cannot leave the board reading Converged"
    );
}

#[tokio::test]
async fn any_stopped_resolves_on_a_dead_supervisor_and_pends_while_they_live() {
    let mut empty = MirrorSupervisor::new().start(Arc::new(Notify::new()));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), empty.any_stopped())
            .await
            .is_err(),
        "with no mirror registered nothing has stopped, so any_stopped never resolves"
    );

    let probe = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(probe.clone(), false))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let mut tasks = supervisor.start(shutdown.clone());
    assert!(
        until(Duration::from_secs(5), || Probe::count(&probe.reconciles)
            >= 1)
        .await,
        "the supervise task must be running before it can be signalled to stop"
    );
    shutdown.notify_waiters();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), tasks.any_stopped())
            .await
            .is_ok(),
        "once a supervise task ends, any_stopped resolves so the engine can force readiness DOWN"
    );
}
