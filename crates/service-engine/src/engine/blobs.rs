use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::blobs::{BlobRef, BlobStore, DownloadUrl};
use crate::engine::Engine;
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::principal::Principal;

#[derive(Clone)]
pub struct BlobReader {
    store: Arc<OnceCell<BlobStore>>,
    pg: PgPool,
}

impl BlobReader {
    pub async fn download_url(
        &self,
        reference: BlobRef,
    ) -> Result<Option<DownloadUrl>, EngineError> {
        match self.store.get() {
            Some(store) => store.download_url(&self.pg, reference).await,
            None => Ok(None),
        }
    }

    pub async fn purge_person(&self, person: PersonId) -> Result<u64, EngineError> {
        match self.store.get() {
            Some(store) => store.purge_person(&self.pg, person).await,
            None => Ok(0),
        }
    }

    pub async fn purge_references(&self, references: &[BlobRef]) -> Result<u64, EngineError> {
        match self.store.get() {
            Some(store) => store.purge_references(&self.pg, references).await,
            None => Ok(0),
        }
    }
}

impl std::fmt::Debug for BlobReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader").finish_non_exhaustive()
    }
}

impl<P: Principal> Engine<P> {
    pub fn blob_reader(&self) -> BlobReader {
        BlobReader {
            store: self.blobs.store_slot(),
            pg: self.pg.clone(),
        }
    }

    pub async fn download_url(
        &self,
        reference: BlobRef,
    ) -> Result<Option<DownloadUrl>, EngineError> {
        self.blob_reader().download_url(reference).await
    }

    pub async fn purge_person_blobs(&self, person: PersonId) -> Result<u64, EngineError> {
        self.blob_reader().purge_person(person).await
    }
}
