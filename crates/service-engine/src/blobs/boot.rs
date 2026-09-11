use crate::blobs::BlobHandle;
use crate::blobs::reaper::BlobReaper;
use crate::blobs::registry::BlobRegistry;
use crate::blobs::store::{REASON_BLOB_BUCKET, REASON_BLOB_UNCONFIGURED};
use crate::config::EngineConfig;
use crate::error::EngineError;

pub(crate) struct BoundBlobs {
    pub handle: Option<BlobHandle>,
    pub reaper: Option<BlobReaper>,
}

pub(crate) async fn bind(
    registry: &BlobRegistry,
    config: &EngineConfig,
) -> Result<BoundBlobs, (EngineError, &'static str)> {
    if registry.is_empty() {
        return Ok(BoundBlobs {
            handle: None,
            reaper: None,
        });
    }
    let store = BlobRegistry::build_store(config.blob.as_ref(), config.blob_service())
        .map_err(|error| (error, REASON_BLOB_UNCONFIGURED))?;
    if let Err(error) = store.object().ensure_bucket().await {
        return Err((error, REASON_BLOB_BUCKET));
    }
    let _ = registry.store_slot().set(store);
    let reaper = BlobReaper::new(registry.store_slot(), registry.policies())
        .with_interval(config.blob_reaper_interval);
    Ok(BoundBlobs {
        handle: registry.maybe_handle(),
        reaper: Some(reaper),
    })
}
