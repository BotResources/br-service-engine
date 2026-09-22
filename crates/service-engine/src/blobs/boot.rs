use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::blobs::BlobHandle;
use crate::blobs::reaper::BlobReaper;
use crate::blobs::registry::BlobRegistry;
use crate::blobs::store::{REASON_BLOB_BUCKET, REASON_BLOB_POLICY, REASON_BLOB_UNCONFIGURED};
use crate::config::EngineConfig;
use crate::error::EngineError;

pub(crate) struct BoundBlobs {
    pub handle: Option<BlobHandle>,
    pub reaper: Option<BlobReaper>,
}

fn orphan_window_covers_upload(orphan_after: Duration, upload_ttl: Duration) -> bool {
    orphan_after >= upload_ttl
}

pub(crate) async fn bind(
    registry: &BlobRegistry,
    config: &EngineConfig,
    policy_kinds: BTreeSet<&'static str>,
) -> Result<BoundBlobs, (EngineError, &'static str)> {
    if registry.is_empty() {
        return Ok(BoundBlobs {
            handle: None,
            reaper: None,
        });
    }
    if let Some(blob) = config.blob.as_ref() {
        for (kind, policy) in registry.policies() {
            if !orphan_window_covers_upload(policy.orphan_after, blob.upload_ttl) {
                return Err((
                    EngineError::BlobOrphanWindowTooShort {
                        kind,
                        orphan_after: policy.orphan_after,
                        upload_ttl: blob.upload_ttl,
                    },
                    REASON_BLOB_POLICY,
                ));
            }
        }
    }
    let store = BlobRegistry::build_store(
        config.blob.as_ref(),
        config.blob_service(),
        Arc::new(policy_kinds),
    )
    .map_err(|error| (error, REASON_BLOB_UNCONFIGURED))?;
    if let Err(error) = store.object().ensure_bucket().await {
        return Err((error, REASON_BLOB_BUCKET));
    }
    let _ = registry.store_slot().set(store);
    let reaper = BlobReaper::new(
        registry.store_slot(),
        registry.policies(),
        config.pod_id.clone(),
        config.lease,
    )
    .with_interval(config.blob_reaper_interval);
    Ok(BoundBlobs {
        handle: registry.maybe_handle(),
        reaper: Some(reaper),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_orphan_window_shorter_than_the_upload_ttl_is_refused() {
        assert!(!orphan_window_covers_upload(
            Duration::from_secs(60),
            Duration::from_secs(900)
        ));
    }

    #[test]
    fn an_orphan_window_equal_to_or_longer_than_the_upload_ttl_is_accepted() {
        assert!(orphan_window_covers_upload(
            Duration::from_secs(900),
            Duration::from_secs(900)
        ));
        assert!(orphan_window_covers_upload(
            Duration::from_secs(86_400),
            Duration::from_secs(900)
        ));
    }
}
