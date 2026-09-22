mod boot;
mod config;
mod expect;
mod handle;
mod head;
mod object;
pub(crate) mod policy;
mod post_policy;
mod reaper;
mod registry;
mod store;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) use boot::{BoundBlobs, bind};
pub use config::BlobConfig;
pub use expect::{Sha256Digest, UploadExpectation};
pub use handle::Blob;
pub(crate) use handle::BlobHandle;
pub use head::{BlobHead, BlobState};
pub(crate) use object::ObjectHead;
pub use policy::{PostUpload, Uploaded};
pub use reaper::ReaperRound;
pub(crate) use reaper::{BlobReaper, ReaperRoundLog};
pub(crate) use registry::BlobRegistry;
pub(crate) use store::{BlobRowOp, BlobStore, insert_reference, orphan_reference};
pub use store::{REASON_BLOB_BUCKET, REASON_BLOB_POLICY, REASON_BLOB_UNCONFIGURED};

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum Disposition {
    Inline,
    Attachment,
}

impl Disposition {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Attachment => "attachment",
        }
    }
}

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
pub struct UploadUrl {
    url: String,
    fields: Vec<(String, String)>,
}

impl UploadUrl {
    pub(crate) fn new(url: String, fields: Vec<(String, String)>) -> Self {
        Self { url, fields }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn fields(&self) -> &[(String, String)] {
        &self.fields
    }

    pub fn into_parts(self) -> (String, Vec<(String, String)>) {
        (self.url, self.fields)
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
