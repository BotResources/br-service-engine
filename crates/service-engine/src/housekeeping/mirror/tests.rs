use super::*;
use crate::mirror::MirrorRun;
use std::time::Duration;

#[derive(Default)]
pub(super) struct Probe {
    pub(super) reconciles: AtomicU64,
    pub(super) watches: AtomicU64,
    pub(super) backfills: AtomicU64,
    pub(super) progress: AtomicU64,
}

impl Probe {
    pub(super) fn count(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::SeqCst)
    }
}

pub(super) fn flapping(probe: Arc<Probe>, advances: bool) -> MirrorHandle {
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
                    probe.watches.fetch_add(1, Ordering::SeqCst);
                    if advances {
                        probe.progress.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(EngineError::Service("the roster stream ended".into()))
                }) as MirrorRun
            }
        },
    )
    .with_backfill({
        let probe = probe.clone();
        move || {
            let probe = probe.clone();
            Box::pin(async move {
                probe.backfills.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }) as MirrorRun
        }
    })
    .with_progress(move || probe.progress.load(Ordering::SeqCst))
}

#[test]
fn two_mirrors_claiming_one_name_would_share_a_health_condition_so_the_second_is_refused() {
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(Arc::new(Probe::default()), false))
        .expect("the first mirror registers");
    let refusal = supervisor.register(flapping(Arc::new(Probe::default()), false));
    assert!(matches!(
        refusal,
        Err(EngineError::DuplicateMirrorName { name }) if name.as_str() == "directory"
    ));
    assert_eq!(
        supervisor.names(),
        vec![MirrorName::from_static("directory")],
        "the refused duplicate never enters the board, so a dead mirror cannot hide behind a \
         same-named live one"
    );
}

pub(super) async fn until(deadline: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let until = tokio::time::Instant::now() + deadline;
    while tokio::time::Instant::now() < until {
        if ready() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ready()
}

#[tokio::test]
async fn a_service_with_no_mirror_is_converged_before_it_starts_anything() {
    let supervisor = MirrorSupervisor::new();
    assert!(supervisor.names().is_empty());
    let mut tasks = supervisor.start(Arc::new(Notify::new()));
    assert!(tasks.converged().await);
    assert_eq!(tasks.restarts(), 0);
}

#[tokio::test]
async fn a_registered_mirror_holds_the_board_back_before_the_supervisor_runs_it() {
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(Arc::new(Probe::default()), false))
        .expect("the mirror registers");
    let board = supervisor.health().borrow().clone();
    assert!(!board.converged());
    assert_eq!(
        board.condition(&MirrorName::from_static("directory")),
        Some(&MirrorCondition::Converging)
    );
}

#[tokio::test]
async fn a_dead_mirror_is_reconciled_again_before_it_is_watched_again() {
    let probe = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(probe.clone(), false))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let tasks = supervisor.start(shutdown.clone());

    assert!(
        until(Duration::from_secs(10), || Probe::count(&probe.watches)
            >= 2)
        .await,
        "the supervisor never restarted the mirror a second time"
    );
    shutdown.notify_waiters();
    let watches = Probe::count(&probe.watches);
    assert_eq!(
        Probe::count(&probe.reconciles),
        watches,
        "every watch is preceded by its own reconcile, so a restart never watches a stale \
         projection"
    );
    assert_eq!(
        Probe::count(&probe.backfills),
        1,
        "the backfill is a one-shot at adoption, not a per-restart pass"
    );
    assert!(tasks.restarts() >= 2);
    assert!(!tasks.is_converged());
}

#[tokio::test]
async fn a_mirror_that_died_reports_why_it_is_being_restarted() {
    let probe = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(probe.clone(), false))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let health = supervisor.health();
    let _tasks = supervisor.start(shutdown.clone());

    assert!(
        until(Duration::from_secs(10), || matches!(
            health
                .borrow()
                .condition(&MirrorName::from_static("directory")),
            Some(MirrorCondition::Restarting { .. })
        ))
        .await,
        "a mirror whose watch returned must leave the converged state"
    );
    let condition = health
        .borrow()
        .condition(&MirrorName::from_static("directory"))
        .cloned()
        .expect("the mirror is on the board");
    assert_eq!(
        condition.reason(),
        Some("service: the roster stream ended"),
        "the whole cause chain reaches the board, not only its top word"
    );
    shutdown.notify_waiters();
}

#[tokio::test]
async fn a_mirror_that_keeps_making_progress_restarts_without_growing_its_backoff() {
    let advancing = Arc::new(Probe::default());
    let stuck = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(flapping(advancing.clone(), true))
        .expect("the mirror registers");
    let mut stalled = MirrorSupervisor::new();
    stalled
        .register(flapping(stuck.clone(), false))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let advancing_board = supervisor.health();
    let stalled_board = stalled.health();
    let _one = supervisor.start(shutdown.clone());
    let _two = stalled.start(shutdown.clone());

    assert!(
        until(Duration::from_secs(15), || Probe::count(&stuck.watches)
            >= 3)
        .await,
        "the stuck mirror never reached its third restart"
    );
    shutdown.notify_waiters();

    let attempts = |health: &MirrorsHealthReceiver| match health
        .borrow()
        .condition(&MirrorName::from_static("directory"))
    {
        Some(MirrorCondition::Restarting { attempts, .. }) => *attempts,
        _ => 0,
    };
    assert_eq!(
        attempts(&advancing_board),
        1,
        "a mirror that mirrored something since its last stop is retried at the base delay"
    );
    assert!(
        attempts(&stalled_board) >= 3,
        "a mirror that mirrored nothing backs further off on every restart"
    );
}

#[tokio::test]
async fn a_shutdown_signalled_while_a_mirror_is_reconciling_is_not_lost() {
    let probe = Arc::new(Probe::default());
    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(MirrorHandle::new(
            MirrorName::from_static("directory"),
            {
                let probe = probe.clone();
                move || {
                    let probe = probe.clone();
                    Box::pin(async move {
                        probe.reconciles.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        Ok(())
                    }) as MirrorRun
                }
            },
            {
                let probe = probe.clone();
                move || {
                    let probe = probe.clone();
                    Box::pin(async move {
                        probe.watches.fetch_add(1, Ordering::SeqCst);
                        Err(EngineError::Service("the roster stream ended".into()))
                    }) as MirrorRun
                }
            },
        ))
        .expect("the mirror registers");
    let shutdown = Arc::new(Notify::new());
    let tasks = supervisor.start(shutdown.clone());

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        Probe::count(&probe.reconciles),
        1,
        "the signal lands while the mirror is mid-reconcile, not while it waits"
    );
    shutdown.notify_waiters();

    assert!(
        until(Duration::from_secs(5), || tasks.is_finished()).await,
        "a shutdown raised while a mirror was working must stop it at the next boundary, not be \
         dropped because nothing was waiting on the notify yet"
    );
    assert!(Probe::count(&probe.watches) <= 1);
}
