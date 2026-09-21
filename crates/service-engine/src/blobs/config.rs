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
    pub public_endpoint: Option<String>,
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
            public_endpoint: None,
            upload_ttl: DEFAULT_UPLOAD_TTL,
            download_ttl: DEFAULT_DOWNLOAD_TTL,
        }
    }

    pub fn with_public_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.public_endpoint = Some(endpoint.into());
        self
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
        if let Some(public) = &self.public_endpoint
            && public.trim().parse::<reqwest::Url>().is_err()
        {
            return Err(EngineError::Config(format!(
                "the blob public endpoint {public} is not a parseable URL"
            )));
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
            .field("public_endpoint", &self.public_endpoint)
            .field("upload_ttl", &self.upload_ttl)
            .field("download_ttl", &self.download_ttl)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BlobConfig {
        BlobConfig::new("https://s3.internal", "eu", "blobs", "AKIA", "secret")
    }

    #[test]
    fn a_well_formed_public_endpoint_validates() {
        let config = config().with_public_endpoint("https://cdn.example.test");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn a_malformed_public_endpoint_is_refused_loud() {
        let config = config().with_public_endpoint("not a url");
        assert!(matches!(config.validate(), Err(EngineError::Config(_))));
    }
}
