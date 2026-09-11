use std::hash::{Hash, Hasher};

use crate::principal::PrincipalId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CohortKey(Box<[u8]>);

const PRINCIPAL_TAG: u8 = 0;
const DECLARED_TAG: u8 = 1;

impl CohortKey {
    pub fn principal(id: PrincipalId) -> Self {
        let mut bytes = Vec::with_capacity(1 + 16);
        bytes.push(PRINCIPAL_TAG);
        bytes.extend_from_slice(id.as_uuid().as_bytes());
        Self(bytes.into())
    }

    pub fn of<H: Hash>(parts: &[H]) -> Self {
        let mut image = ByteImage(vec![DECLARED_TAG]);
        parts.len().hash(&mut image);
        for part in parts {
            part.hash(&mut image);
        }
        Self(image.0.into())
    }

    #[cfg(test)]
    pub(crate) fn image(&self) -> &[u8] {
        &self.0
    }
}

struct ByteImage(Vec<u8>);

impl Hasher for ByteImage {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn an_rls_cohort_key_is_a_lossless_image_of_the_full_principal_id() {
        let id = Uuid::now_v7();
        let key = CohortKey::principal(PrincipalId::from(id));
        assert!(
            key.image().windows(16).any(|w| w == id.as_bytes()),
            "the exact 128-bit principal id must survive in the key, never a truncating hash"
        );
    }

    #[test]
    fn two_principals_sharing_a_64_bit_half_still_render_in_separate_rls_groups() {
        let low = 0x0123_4567_89ab_cdef_u128;
        let a = PrincipalId::from(Uuid::from_u128((0x1111_1111_1111_1111_u128 << 64) | low));
        let b = PrincipalId::from(Uuid::from_u128((0x2222_2222_2222_2222_u128 << 64) | low));
        assert_ne!(a, b);
        assert_ne!(CohortKey::principal(a), CohortKey::principal(b));
        assert_eq!(CohortKey::principal(a), CohortKey::principal(a));
    }

    #[test]
    fn a_declared_cohort_key_carries_the_exact_bytes_of_its_parts() {
        let tenant = Uuid::now_v7();
        let key = CohortKey::of(&[tenant]);
        assert!(
            key.image().windows(16).any(|w| w == tenant.as_bytes()),
            "a declared cohort key must be a lossless image, never a 64-bit hash bucket"
        );
    }

    #[test]
    fn the_default_cohort_shares_nothing_between_two_principals() {
        let a = PrincipalId::from(Uuid::now_v7());
        let b = PrincipalId::from(Uuid::now_v7());
        assert_ne!(CohortKey::principal(a), CohortKey::principal(b));
        assert_eq!(CohortKey::principal(a), CohortKey::principal(a));
    }

    #[test]
    fn two_principals_that_render_identically_share_one_coarse_cohort() {
        let tenant = Uuid::now_v7();
        assert_eq!(CohortKey::of(&[tenant]), CohortKey::of(&[tenant]));
        assert_ne!(CohortKey::of(&[tenant]), CohortKey::of(&[Uuid::now_v7()]));
    }

    #[test]
    fn a_principal_key_and_a_declared_key_never_collide() {
        let id = Uuid::now_v7();
        let principal = CohortKey::principal(PrincipalId::from(id));
        let declared = CohortKey::of(&[id]);
        assert_ne!(principal, declared);
    }

    #[test]
    fn cohort_parts_are_length_prefixed_so_a_split_cannot_collide() {
        assert_ne!(
            CohortKey::of(&["ab".to_string(), String::new()]),
            CohortKey::of(&["a".to_string(), "b".to_string()])
        );
    }
}
