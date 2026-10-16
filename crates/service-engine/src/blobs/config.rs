use std::time::Duration;

use crate::error::EngineError;

pub const DEFAULT_UPLOAD_TTL: Duration = Duration::from_secs(15 * 60);
pub const DEFAULT_DOWNLOAD_TTL: Duration = Duration::from_secs(15 * 60);
const MIN_UPLOAD_TTL: Duration = Duration::from_millis(1);
const MIN_DOWNLOAD_TTL: Duration = Duration::from_secs(1);
const MAX_PRESIGN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

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

    pub(crate) fn validate_ttls(&self) -> Result<(), EngineError> {
        if self.upload_ttl < MIN_UPLOAD_TTL || self.upload_ttl > MAX_PRESIGN_TTL {
            return Err(EngineError::Config(format!(
                "blob upload_ttl must lie between {MIN_UPLOAD_TTL:?} and {MAX_PRESIGN_TTL:?}, \
                 otherwise the signed POST policy expires at once or overflows its expiry"
            )));
        }
        if self.download_ttl < MIN_DOWNLOAD_TTL || self.download_ttl > MAX_PRESIGN_TTL {
            return Err(EngineError::Config(format!(
                "blob download_ttl must lie between {MIN_DOWNLOAD_TTL:?} and {MAX_PRESIGN_TTL:?}, \
                 the whole-second range S3 accepts for X-Amz-Expires"
            )));
        }
        Ok(())
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
        self.validate_ttls()?;
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

    fn refused_ttls(config: BlobConfig) -> String {
        match config.validate_ttls() {
            Err(EngineError::Config(message)) => message,
            other => panic!("the TTL must be refused as a configuration error, got {other:?}"),
        }
    }

    #[test]
    fn a_download_ttl_below_one_second_is_refused_naming_it() {
        for ttl in [Duration::ZERO, Duration::from_millis(999)] {
            let message = refused_ttls(config().with_download_ttl(ttl));
            assert!(message.contains("download_ttl"), "{message}");
        }
        config()
            .with_download_ttl(Duration::from_secs(1))
            .validate_ttls()
            .expect("one second is the smallest X-Amz-Expires");
    }

    #[test]
    fn a_download_ttl_above_seven_days_is_refused() {
        config()
            .with_download_ttl(MAX_PRESIGN_TTL)
            .validate_ttls()
            .expect("seven days is the largest X-Amz-Expires");
        let message =
            refused_ttls(config().with_download_ttl(MAX_PRESIGN_TTL + Duration::from_nanos(1)));
        assert!(message.contains("download_ttl"), "{message}");
    }

    #[test]
    fn an_upload_ttl_below_one_millisecond_is_refused_naming_it() {
        for ttl in [Duration::ZERO, Duration::from_micros(999)] {
            let message = refused_ttls(config().with_upload_ttl(ttl));
            assert!(message.contains("upload_ttl"), "{message}");
        }
        config()
            .with_upload_ttl(Duration::from_millis(1))
            .validate_ttls()
            .expect("one millisecond is the finest POST-policy expiry");
    }

    #[test]
    fn an_upload_ttl_that_overflows_the_policy_expiry_is_refused() {
        config()
            .with_upload_ttl(MAX_PRESIGN_TTL)
            .validate_ttls()
            .expect("seven days validates");
        let message = refused_ttls(config().with_upload_ttl(Duration::from_secs(u64::MAX)));
        assert!(message.contains("upload_ttl"), "{message}");
    }

    #[test]
    fn a_malformed_public_endpoint_is_refused_loud() {
        let config = config().with_public_endpoint("not a url");
        assert!(matches!(config.validate(), Err(EngineError::Config(_))));
    }
}
