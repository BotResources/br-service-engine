mod config;
mod handle;
mod object;
mod reaper;
mod registry;
mod store;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use config::BlobConfig;
pub use handle::Blob;
pub(crate) use handle::BlobHandle;
pub(crate) use reaper::BlobReaper;
pub use reaper::ReaperRound;
pub(crate) use registry::BlobRegistry;
pub(crate) use store::{BlobRowOp, BlobStore, insert_reference, orphan_reference};
pub use store::{REASON_BLOB_BUCKET, REASON_BLOB_UNCONFIGURED};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPolicy {
    pub max_bytes: u64,
    pub orphan_after: Duration,
}

pub trait Blobs: Send + Sync + 'static {
    const KIND: &'static str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobRef(pub Uuid);

impl BlobRef {
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadUrl(String);

impl UploadUrl {
    pub(crate) fn new(url: String) -> Self {
        Self(url)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadUrl(String);

impl DownloadUrl {
    pub(crate) fn new(url: String) -> Self {
        Self(url)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}
