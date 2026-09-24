use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::blobs::BlobStore;
use crate::pipeline::MutationExecutor;
use crate::principal::Principal;
use crate::runtime::SessionRuntime;

pub struct GraphqlState<P: Principal> {
    executor: MutationExecutor<P>,
    runtime: Arc<SessionRuntime<P>>,
    pg: PgPool,
    blobs: Arc<OnceCell<BlobStore>>,
    stream_shutdown: tokio::sync::watch::Receiver<bool>,
}

impl<P: Principal> GraphqlState<P> {
    pub(crate) fn new(
        executor: MutationExecutor<P>,
        runtime: Arc<SessionRuntime<P>>,
        pg: PgPool,
        blobs: Arc<OnceCell<BlobStore>>,
        stream_shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            executor,
            runtime,
            pg,
            blobs,
            stream_shutdown,
        }
    }

    pub fn pg(&self) -> &PgPool {
        &self.pg
    }

    pub(crate) fn stream_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stream_shutdown.clone()
    }

    pub(crate) fn blob_store(&self) -> Option<&BlobStore> {
        self.blobs.get()
    }

    pub(crate) fn executor(&self) -> &MutationExecutor<P> {
        &self.executor
    }

    pub(crate) fn runtime(&self) -> &Arc<SessionRuntime<P>> {
        &self.runtime
    }
}

impl<P: Principal> std::fmt::Debug for GraphqlState<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphqlState").finish_non_exhaustive()
    }
}
