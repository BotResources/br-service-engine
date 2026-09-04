#[cfg(feature = "directory")]
pub mod directory;
mod health;
#[cfg(test)]
mod liveness_tests;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures_util::FutureExt;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::chain::describe;
use crate::error::EngineError;
use crate::housekeeping::backoff::Backoff;
use crate::mirror::MirrorHandle;
use crate::name::MirrorName;

pub use health::{MirrorCondition, MirrorsHealth, MirrorsHealthReceiver};

type Board = Arc<watch::Sender<MirrorsHealth>>;

pub struct MirrorSupervisor {
    mirrors: Vec<MirrorHandle>,
    board: Board,
    subscription: MirrorsHealthReceiver,
    restarts: Arc<AtomicU64>,
}

impl Default for MirrorSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl MirrorSupervisor {
    pub fn new() -> Self {
        let (board, subscription) = watch::channel(MirrorsHealth::default());
        Self {
            mirrors: Vec::new(),
            board: Arc::new(board),
            subscription,
            restarts: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn register(&mut self, mirror: MirrorHandle) -> Result<(), EngineError> {
        let name = mirror.name().clone();
        if self.mirrors.iter().any(|held| held.name() == &name) {
            return Err(EngineError::DuplicateMirrorName { name });
        }
        set(&self.board, &name, MirrorCondition::Converging);
        self.mirrors.push(mirror);
        Ok(())
    }

    pub fn names(&self) -> Vec<MirrorName> {
        self.mirrors.iter().map(|m| m.name().clone()).collect()
    }

    pub fn health(&self) -> MirrorsHealthReceiver {
        self.subscription.clone()
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub fn start(self, shutdown: Arc<Notify>) -> MirrorTasks {
        let tasks = self
            .mirrors
            .iter()
            .map(|mirror| {
                tokio::spawn(supervise(
                    mirror.clone(),
                    self.board.clone(),
                    self.restarts.clone(),
                    shutdown.clone(),
                ))
            })
            .collect();
        MirrorTasks {
            tasks,
            health: self.subscription.clone(),
            restarts: self.restarts.clone(),
        }
    }
}

pub struct MirrorTasks {
    tasks: Vec<JoinHandle<()>>,
    health: MirrorsHealthReceiver,
    restarts: Arc<AtomicU64>,
}

impl MirrorTasks {
    pub fn health(&self) -> MirrorsHealthReceiver {
        self.health.clone()
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub fn is_converged(&self) -> bool {
        self.health.borrow().converged()
    }

    pub async fn converged(&mut self) -> bool {
        loop {
            if self.health.borrow_and_update().converged() {
                return true;
            }
            if self.health.changed().await.is_err() {
                return false;
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        self.tasks.iter().all(JoinHandle::is_finished)
    }

    pub async fn any_stopped(&mut self) {
        if self.tasks.is_empty() {
            std::future::pending::<()>().await;
        }
        let _ = futures_util::future::select_all(self.tasks.iter_mut()).await;
    }

    pub fn abort(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Drop for MirrorTasks {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn supervise(
    mirror: MirrorHandle,
    board: Board,
    restarts: Arc<AtomicU64>,
    shutdown: Arc<Notify>,
) {
    let name = mirror.name().clone();
    let mut backoff = Backoff::default();
    let mut progress_at_last_stop = mirror.progress();
    let mut backfilled = false;
    let stopping = shutdown.notified();
    tokio::pin!(stopping);
    stopping.as_mut().enable();
    loop {
        set(&board, &name, MirrorCondition::Converging);
        let reason = match guard(converge(&mirror, &mut backfilled)).await {
            Err(panic) => panic,
            Ok(Err(reason)) => reason,
            Ok(Ok(())) => {
                set(&board, &name, MirrorCondition::Converged);
                tokio::select! {
                    biased;
                    () = &mut stopping => return,
                    outcome = guard(mirror.watch()) => match outcome {
                        Err(panic) => panic,
                        Ok(Ok(())) => "the mirror watch returned".to_string(),
                        Ok(Err(error)) => describe(&error),
                    },
                }
            }
        };
        restarts.fetch_add(1, Ordering::Relaxed);
        crate::observe::record_mirror_restart(&name);
        let progress = mirror.progress();
        if progress > progress_at_last_stop {
            backoff.succeed();
        }
        progress_at_last_stop = progress;
        let retry_in = backoff.fail(Instant::now());
        tracing::warn!(
            mirror = %name,
            attempts = backoff.attempts(),
            retry_in_ms = retry_in.as_millis(),
            reason = %reason,
            "a mirror stopped and is being restarted",
        );
        set(
            &board,
            &name,
            MirrorCondition::Restarting {
                attempts: backoff.attempts(),
                retry_in,
                reason,
            },
        );
        tokio::select! {
            biased;
            () = &mut stopping => return,
            () = tokio::time::sleep(retry_in) => {}
        }
    }
}

async fn guard<T>(step: impl Future<Output = T>) -> Result<T, String> {
    AssertUnwindSafe(step)
        .catch_unwind()
        .await
        .map_err(|_| "the mirror panicked and is treated as a stopped mirror".to_string())
}

async fn converge(mirror: &MirrorHandle, backfilled: &mut bool) -> Result<(), String> {
    mirror.reconcile().await.map_err(describe_engine)?;
    if !*backfilled {
        if let Some(backfill) = mirror.backfill() {
            backfill.await.map_err(describe_engine)?;
        }
        *backfilled = true;
    }
    Ok(())
}

fn describe_engine(error: EngineError) -> String {
    describe(&error)
}

fn set(board: &Board, mirror: &MirrorName, condition: MirrorCondition) {
    board.send_modify(|health| health.set(mirror.clone(), condition));
}
