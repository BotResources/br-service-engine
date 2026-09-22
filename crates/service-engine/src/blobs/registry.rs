use std::any::TypeId;
use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::blobs::config::BlobConfig;
use crate::blobs::handle::BlobHandle;
use crate::blobs::object::ObjectStore;
use crate::blobs::store::BlobStore;
use crate::blobs::{BlobPolicy, Blobs};
use crate::error::EngineError;

pub(crate) struct BlobRegistry {
    kinds: Vec<(TypeId, &'static str, BlobPolicy)>,
    store: Arc<OnceCell<BlobStore>>,
}

impl BlobRegistry {
    pub(crate) fn new() -> Self {
        Self {
            kinds: Vec::new(),
            store: Arc::new(OnceCell::new()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    pub(crate) fn register<B: Blobs>(&mut self, policy: BlobPolicy) -> Result<(), EngineError> {
        validate_kind(B::KIND)?;
        let type_id = TypeId::of::<B>();
        if self.kinds.iter().any(|(id, _, _)| *id == type_id) {
            return Err(EngineError::Blob(format!(
                "blob kind {} is already registered",
                B::KIND
            )));
        }
        if self.kinds.iter().any(|(_, kind, _)| *kind == B::KIND) {
            return Err(EngineError::Blob(format!(
                "blob kind name {} is already held by another blob type",
                B::KIND
            )));
        }
        self.kinds.push((type_id, B::KIND, policy));
        Ok(())
    }

    pub(crate) fn handle(&self) -> BlobHandle {
        BlobHandle::new(Arc::new(self.kinds.clone()), self.store.clone())
    }

    pub(crate) fn maybe_handle(&self) -> Option<BlobHandle> {
        if self.is_empty() {
            None
        } else {
            Some(self.handle())
        }
    }

    pub(crate) fn store_slot(&self) -> Arc<OnceCell<BlobStore>> {
        self.store.clone()
    }

    pub(crate) fn policies(&self) -> Vec<(&'static str, BlobPolicy)> {
        self.kinds
            .iter()
            .map(|(_, kind, policy)| (*kind, *policy))
            .collect()
    }

    pub(crate) fn build_store(
        config: Option<&BlobConfig>,
        service: &str,
        policy_kinds: Arc<std::collections::BTreeSet<&'static str>>,
    ) -> Result<BlobStore, EngineError> {
        let config = config.ok_or_else(|| {
            EngineError::Blob(
                "a blob kind is registered but no object storage is configured; call \
                 EngineConfig::with_blob_storage"
                    .into(),
            )
        })?;
        let object = Arc::new(ObjectStore::from_config(config)?);
        Ok(BlobStore::new(object, service, policy_kinds))
    }
}

fn validate_kind(kind: &str) -> Result<(), EngineError> {
    let shaped = !kind.is_empty()
        && kind
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if shaped {
        Ok(())
    } else {
        Err(EngineError::Blob(format!(
            "blob kind {kind} must be a non-empty slug of lowercase letters, digits, '_' or '-'"
        )))
    }
}
