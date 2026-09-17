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
use tokio::sync::watch;

use tokio::task::JoinHandle;

use uuid::Uuid;

use crate::chain::describe;
use crate::error::EngineError;
use crate::housekeeping::backoff::Backoff;
use crate::inbound::{DeadLetterSource, DeadLetters};
use crate::mirror::MirrorHandle;
use crate::name::MirrorName;
use crate::stop::Stop;

pub use health::{MirrorCondition, MirrorsHealth, MirrorsHealthReceiver};

type Board = Arc<watch::Sender<MirrorsHealth>>;

const MIRROR_DEAD_LETTER_THRESHOLD: u32 = 5;
const MIRROR_DEAD_LETTER_NAMESPACE: Uuid =
    Uuid::from_u128(0x91130000_c0c07100_b0000000_dead1e77u128);

pub struct MirrorSupervisor {
    mirrors: Vec<MirrorHandle>,
    board: Board,
    subscription: MirrorsHealthReceiver,
    restarts: Arc<AtomicU64>,
    dead_letters: Option<DeadLetters>,
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
            dead_letters: None,
        }
    }

    pub fn set_dead_letters(&mut self, dead_letters: DeadLetters) {
        self.dead_letters = Some(dead_letters);
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

    pub fn required_keys(&self) -> Vec<watch::Receiver<Vec<String>>> {
        self.mirrors
            .iter()
            .filter_map(MirrorHandle::required_keys)
            .collect()
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub fn start(self, shutdown: Arc<Stop>) -> MirrorTasks {
        let tasks = self
            .mirrors
            .iter()
            .map(|mirror| {
                tokio::spawn(supervise(
                    mirror.clone(),
                    self.board.clone(),
                    self.restarts.clone(),
                    self.dead_letters.clone(),
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
    dead_letters: Option<DeadLetters>,
    shutdown: Arc<Stop>,
) {
    let name = mirror.name().clone();
    let mut backoff = Backoff::default();
    let mut progress_at_last_stop = mirror.progress();
    let mut backfilled = false;
    let stopping = shutdown.stopped();
    tokio::pin!(stopping);
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
        if backoff.attempts() >= MIRROR_DEAD_LETTER_THRESHOLD {
            record_mirror_dead_letter(dead_letters.as_ref(), &name, &reason).await;
        }
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

async fn record_mirror_dead_letter(
    dead_letters: Option<&DeadLetters>,
    name: &MirrorName,
    reason: &str,
) {
    let Some(dead_letters) = dead_letters else {
        return;
    };
    let message_id = Uuid::new_v5(&MIRROR_DEAD_LETTER_NAMESPACE, name.as_str().as_bytes());
    if let Err(error) = dead_letters
        .record_work(
            DeadLetterSource::Mirror,
            name.as_str(),
            name.as_str(),
            message_id,
            reason,
        )
        .await
    {
        tracing::warn!(
            mirror = %name,
            reason = %describe(&error),
            "a stuck mirror could not be written to the dead-letter table",
        );
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
