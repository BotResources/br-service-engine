use std::time::Duration;

use crate::error::EngineError;

pub const DEFAULT_UPLOAD_TTL: Duration = Duration::from_secs(15 * 60);
pub const DEFAULT_DOWNLOAD_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Clone)]
pub struct BlobConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub upload_ttl: Duration,
    pub download_ttl: Duration,
}

impl BlobConfig {
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            upload_ttl: DEFAULT_UPLOAD_TTL,
            download_ttl: DEFAULT_DOWNLOAD_TTL,
        }
    }

    pub fn with_upload_ttl(mut self, ttl: Duration) -> Self {
        self.upload_ttl = ttl;
        self
    }

    pub fn with_download_ttl(mut self, ttl: Duration) -> Self {
        self.download_ttl = ttl;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), EngineError> {
        let blank = self.endpoint.trim().is_empty()
            || self.bucket.trim().is_empty()
            || self.access_key.trim().is_empty()
            || self.secret_key.trim().is_empty();
        if blank {
            return Err(EngineError::Config(
                "object storage is configured with a blank endpoint, bucket or credential".into(),
            ));
        }
        if self.upload_ttl.is_zero() || self.download_ttl.is_zero() {
            return Err(EngineError::Config(
                "a presign TTL of zero would mint a URL that is already expired".into(),
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for BlobConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("upload_ttl", &self.upload_ttl)
            .field("download_ttl", &self.download_ttl)
            .finish_non_exhaustive()
    }
}
