use crate::config::EngineConfig;
use crate::error::EngineError;

impl EngineConfig {
    pub(super) fn validate_body_bounds(&self) -> Result<(), EngineError> {
        if self.max_body_bytes == 0 {
            return Err(EngineError::Config(
                "max_body_bytes must be non-zero, or every authenticated body would be refused"
                    .into(),
            ));
        }
        match self.multipart.max_file_bytes {
            Some(0) => Err(EngineError::Config(
                "multipart max_file_bytes must be non-zero, or no multipart request could carry \
                 even its `operations` part"
                    .into(),
            )),
            Some(part) if part > self.max_body_bytes => Err(EngineError::Config(format!(
                "multipart max_file_bytes ({part}) must not exceed max_body_bytes ({}): a part is \
                 part of the body; lower MultipartConfig::max_file_bytes, or leave it unset to \
                 follow max_body_bytes",
                self.max_body_bytes
            ))),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EngineConfig;
    use crate::graphql::MultipartConfig;
    use crate::name::{ChannelName, PodId};

    const MIB: u64 = 1024 * 1024;

    fn config() -> EngineConfig {
        EngineConfig::new(
            ChannelName::new("service_engine_impact").unwrap(),
            PodId::new("svc-sample-0").unwrap(),
        )
    }

    fn with_file_bound(max_file_bytes: u64) -> MultipartConfig {
        MultipartConfig::default().with_max_file_bytes(max_file_bytes)
    }

    #[test]
    fn a_fresh_config_bounds_every_body_at_sixteen_mebibytes() {
        let config = config();

        assert_eq!(config.max_body_bytes, 16 * MIB);
        config.validate().expect("the default body bounds validate");
    }

    #[test]
    fn a_zero_body_bound_is_refused() {
        assert!(config().with_max_body_bytes(0).validate().is_err());
    }

    #[test]
    fn a_body_bound_below_the_default_part_bound_is_set_alone() {
        config()
            .with_max_body_bytes(MIB)
            .validate()
            .expect("an unset part bound follows the body bound down");
    }

    #[test]
    fn an_explicit_part_bound_above_the_body_bound_is_refused() {
        let raised_part_bound = config().with_multipart(with_file_bound(16 * MIB + 1));
        assert!(raised_part_bound.validate().is_err());

        let lowered_body_bound = config()
            .with_max_body_bytes(MIB)
            .with_multipart(with_file_bound(2 * MIB));
        assert!(lowered_body_bound.validate().is_err());
    }

    #[test]
    fn a_multipart_part_bound_equal_to_the_body_bound_is_accepted() {
        config()
            .with_max_body_bytes(MIB)
            .with_multipart(with_file_bound(MIB))
            .validate()
            .expect("a single part may fill the whole body");
    }

    #[test]
    fn a_zero_multipart_part_bound_is_refused() {
        assert!(
            config()
                .with_multipart(with_file_bound(0))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn zero_uploads_is_a_valid_multipart_policy() {
        config()
            .with_multipart(MultipartConfig::default().with_max_files(0))
            .validate()
            .expect("multipart without uploads is a valid policy");
    }
}
