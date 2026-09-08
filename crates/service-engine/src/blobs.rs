use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPolicy {
    pub max_bytes: u64,
    pub orphan_after: Duration,
}

pub trait Blobs: Send + Sync + 'static {
    type Reference;
}
