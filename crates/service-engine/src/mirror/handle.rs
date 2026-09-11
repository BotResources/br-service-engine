use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::error::EngineError;
use crate::name::MirrorName;

pub type MirrorRun = BoxFuture<'static, Result<(), EngineError>>;

type MirrorStep = Arc<dyn Fn() -> MirrorRun + Send + Sync>;
type MirrorProgress = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone)]
pub struct MirrorHandle {
    name: MirrorName,
    reconcile: MirrorStep,
    watch: MirrorStep,
    backfill: Option<MirrorStep>,
    progress: Option<MirrorProgress>,
}

impl MirrorHandle {
    pub fn new(
        name: MirrorName,
        reconcile: impl Fn() -> MirrorRun + Send + Sync + 'static,
        watch: impl Fn() -> MirrorRun + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            reconcile: Arc::new(reconcile),
            watch: Arc::new(watch),
            backfill: None,
            progress: None,
        }
    }

    pub fn with_backfill(
        mut self,
        backfill: impl Fn() -> MirrorRun + Send + Sync + 'static,
    ) -> Self {
        self.backfill = Some(Arc::new(backfill));
        self
    }

    pub fn with_progress(mut self, progress: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(progress));
        self
    }

    pub fn name(&self) -> &MirrorName {
        &self.name
    }

    pub fn reconcile(&self) -> MirrorRun {
        (self.reconcile)()
    }

    pub fn watch(&self) -> MirrorRun {
        (self.watch)()
    }

    pub fn backfill(&self) -> Option<MirrorRun> {
        self.backfill.as_ref().map(|backfill| backfill())
    }

    pub fn reports_progress(&self) -> bool {
        self.progress.is_some()
    }

    pub fn progress(&self) -> u64 {
        self.progress.as_ref().map_or(0, |progress| progress())
    }
}

impl std::fmt::Debug for MirrorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorHandle")
            .field("name", &self.name)
            .field("backfill", &self.backfill.is_some())
            .field("progress", &self.progress.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn a_mirror_handle_replays_its_steps_so_a_dead_watch_can_be_restarted() {
        let handle = MirrorHandle::new(
            MirrorName::new("directory").unwrap(),
            || Box::pin(async { Ok(()) }),
            || Box::pin(async { Err(EngineError::Config("watch died".into())) }),
        );
        assert_eq!(handle.name().as_str(), "directory");
        assert!(handle.reconcile().await.is_ok());
        assert!(handle.watch().await.is_err());
        assert!(handle.watch().await.is_err());
    }

    #[tokio::test]
    async fn a_handle_with_no_backfill_and_no_progress_reports_neither_rather_than_pretending() {
        let handle = MirrorHandle::new(
            MirrorName::new("directory").unwrap(),
            || Box::pin(async { Ok(()) }),
            || Box::pin(async { Ok(()) }),
        );
        assert!(handle.backfill().is_none());
        assert!(!handle.reports_progress());
        assert_eq!(handle.progress(), 0);
    }

    #[tokio::test]
    async fn a_registered_backfill_and_progress_read_the_service_state_on_every_call() {
        let changes = Arc::new(AtomicU64::new(0));
        let backfilled = Arc::new(AtomicU64::new(0));
        let handle = MirrorHandle::new(
            MirrorName::new("directory").unwrap(),
            || Box::pin(async { Ok(()) }),
            || Box::pin(async { Ok(()) }),
        )
        .with_backfill({
            let backfilled = backfilled.clone();
            move || {
                let backfilled = backfilled.clone();
                Box::pin(async move {
                    backfilled.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }) as MirrorRun
            }
        })
        .with_progress({
            let changes = changes.clone();
            move || changes.load(Ordering::SeqCst)
        });

        assert!(handle.reports_progress());
        assert_eq!(handle.progress(), 0);
        changes.store(7, Ordering::SeqCst);
        assert_eq!(handle.progress(), 7);
        handle
            .backfill()
            .expect("a registered backfill")
            .await
            .unwrap();
        assert_eq!(backfilled.load(Ordering::SeqCst), 1);
    }
}
