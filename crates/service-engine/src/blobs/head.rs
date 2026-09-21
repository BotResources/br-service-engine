use crate::blobs::expect::{Sha256Digest, UploadExpectation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobState {
    Pending,
    Uploaded,
    Orphaned,
    Failed,
}

impl BlobState {
    pub(crate) fn from_row(state: &str) -> Self {
        match state {
            "pending" => Self::Pending,
            "uploaded" => Self::Uploaded,
            "orphaned" => Self::Orphaned,
            _ => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobHead {
    pub state: BlobState,
    pub expected: Option<UploadExpectation>,
    pub size: u64,
    pub etag: String,
    pub sha256: Option<Sha256Digest>,
}

impl BlobHead {
    pub fn verified(&self) -> Option<bool> {
        let expected = self.expected?;
        Some(self.size == expected.size && self.sha256 == Some(expected.sha256))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Sha256Digest {
        Sha256Digest::from_bytes([byte; 32])
    }

    fn head(size: u64, sha: Option<Sha256Digest>, expected: Option<UploadExpectation>) -> BlobHead {
        BlobHead {
            state: BlobState::Pending,
            expected,
            size,
            etag: "etag".into(),
            sha256: sha,
        }
    }

    #[test]
    fn a_head_with_no_expectation_cannot_say_it_is_verified() {
        assert_eq!(head(10, Some(digest(1)), None).verified(), None);
    }

    #[test]
    fn a_matching_size_and_checksum_verifies() {
        let expected = UploadExpectation::new(10, digest(1));
        assert_eq!(
            head(10, Some(digest(1)), Some(expected)).verified(),
            Some(true)
        );
    }

    #[test]
    fn a_size_mismatch_does_not_verify() {
        let expected = UploadExpectation::new(10, digest(1));
        assert_eq!(
            head(11, Some(digest(1)), Some(expected)).verified(),
            Some(false)
        );
    }

    #[test]
    fn a_checksum_mismatch_or_absent_checksum_does_not_verify() {
        let expected = UploadExpectation::new(10, digest(1));
        assert_eq!(
            head(10, Some(digest(2)), Some(expected)).verified(),
            Some(false)
        );
        assert_eq!(head(10, None, Some(expected)).verified(), Some(false));
    }

    #[test]
    fn the_row_state_maps_from_its_stored_text_and_an_unknown_text_fails_closed() {
        assert_eq!(BlobState::from_row("pending"), BlobState::Pending);
        assert_eq!(BlobState::from_row("uploaded"), BlobState::Uploaded);
        assert_eq!(BlobState::from_row("orphaned"), BlobState::Orphaned);
        assert_eq!(BlobState::from_row("failed"), BlobState::Failed);
        assert_eq!(BlobState::from_row("weird"), BlobState::Failed);
    }
}
