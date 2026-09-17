use uuid::Uuid;

use crate::principal::PrincipalId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CohortKey(Box<[u8]>);

const PRINCIPAL_TAG: u8 = 0;
const DECLARED_TAG: u8 = 1;

const VALUE_UUID: u8 = 0;
const VALUE_TEXT: u8 = 1;
const VALUE_BOOL: u8 = 2;
const VALUE_INT: u8 = 3;

impl CohortKey {
    pub fn principal(id: PrincipalId) -> Self {
        let mut bytes = Vec::with_capacity(1 + 16);
        bytes.push(PRINCIPAL_TAG);
        bytes.extend_from_slice(id.as_uuid().as_bytes());
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn image(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum CohortValue {
    Uuid(Uuid),
    Text(String),
    Bool(bool),
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cohort {
    dimension: &'static str,
    value: CohortValue,
}

impl Cohort {
    pub fn uuid(dimension: &'static str, id: Uuid) -> Self {
        Self {
            dimension,
            value: CohortValue::Uuid(id),
        }
    }

    pub fn text(dimension: &'static str, value: impl Into<String>) -> Self {
        Self {
            dimension,
            value: CohortValue::Text(value.into()),
        }
    }

    pub fn flag(dimension: &'static str, value: bool) -> Self {
        Self {
            dimension,
            value: CohortValue::Bool(value),
        }
    }

    pub fn int(dimension: &'static str, value: i64) -> Self {
        Self {
            dimension,
            value: CohortValue::Int(value),
        }
    }

    pub fn dimension(&self) -> &'static str {
        self.dimension
    }

    pub fn value(&self) -> &CohortValue {
        &self.value
    }

    pub fn key(&self) -> CohortKey {
        let mut bytes = vec![DECLARED_TAG];
        let dimension = self.dimension.as_bytes();
        bytes.extend_from_slice(&(dimension.len() as u64).to_le_bytes());
        bytes.extend_from_slice(dimension);
        match &self.value {
            CohortValue::Uuid(id) => {
                bytes.push(VALUE_UUID);
                bytes.extend_from_slice(id.as_bytes());
            }
            CohortValue::Text(text) => {
                bytes.push(VALUE_TEXT);
                let text = text.as_bytes();
                bytes.extend_from_slice(&(text.len() as u64).to_le_bytes());
                bytes.extend_from_slice(text);
            }
            CohortValue::Bool(flag) => {
                bytes.push(VALUE_BOOL);
                bytes.push(u8::from(*flag));
            }
            CohortValue::Int(value) => {
                bytes.push(VALUE_INT);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        CohortKey(bytes.into())
    }

    pub fn uuids(cohorts: &[Cohort], dimension: &str) -> Vec<Uuid> {
        cohorts
            .iter()
            .filter(|cohort| cohort.dimension == dimension)
            .filter_map(|cohort| match cohort.value {
                CohortValue::Uuid(id) => Some(id),
                _ => None,
            })
            .collect()
    }

    pub fn texts(cohorts: &[Cohort], dimension: &str) -> Vec<String> {
        cohorts
            .iter()
            .filter(|cohort| cohort.dimension == dimension)
            .filter_map(|cohort| match &cohort.value {
                CohortValue::Text(text) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn holds(cohorts: &[Cohort], dimension: &str, flag: bool) -> bool {
        cohorts.iter().any(|cohort| {
            cohort.dimension == dimension
                && matches!(cohort.value, CohortValue::Bool(value) if value == flag)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_declared_cohort_key_carries_the_exact_bytes_of_its_value() {
        let tenant = Uuid::now_v7();
        let key = Cohort::uuid("tenant", tenant).key();
        assert!(
            key.image().windows(16).any(|w| w == tenant.as_bytes()),
            "a declared cohort key is a lossless image of its value, never a 64-bit hash bucket"
        );
    }

    #[test]
    fn a_principal_key_and_a_declared_key_never_collide() {
        let id = Uuid::now_v7();
        let principal = CohortKey::principal(PrincipalId::from(id));
        let declared = Cohort::uuid("tenant", id).key();
        assert_ne!(principal, declared);
    }

    #[test]
    fn two_equal_cohorts_render_identically_and_a_changed_value_diverges() {
        let tenant = Uuid::now_v7();
        assert_eq!(
            Cohort::uuid("tenant", tenant),
            Cohort::uuid("tenant", tenant)
        );
        assert_eq!(
            Cohort::uuid("tenant", tenant).key(),
            Cohort::uuid("tenant", tenant).key()
        );
        assert_ne!(
            Cohort::uuid("tenant", tenant).key(),
            Cohort::uuid("tenant", Uuid::now_v7()).key()
        );
    }

    #[test]
    fn the_same_value_under_two_dimensions_never_collides() {
        let id = Uuid::now_v7();
        assert_ne!(
            Cohort::uuid("manager", id).key(),
            Cohort::uuid("member", id).key()
        );
        assert_ne!(Cohort::uuid("manager", id), Cohort::uuid("member", id));
    }

    #[test]
    fn a_dimension_and_value_split_cannot_collide() {
        assert_ne!(Cohort::text("ab", "c").key(), Cohort::text("a", "bc").key());
    }

    #[test]
    fn distinct_value_types_never_collide() {
        assert_ne!(Cohort::text("n", "1").key(), Cohort::int("n", 1).key());
        assert_ne!(Cohort::flag("n", true).key(), Cohort::int("n", 1).key());
    }

    #[test]
    fn uuids_extracts_only_the_named_dimensions_uuid_values() {
        let mine = Uuid::now_v7();
        let other = Uuid::now_v7();
        let cohorts = vec![
            Cohort::uuid("manager", mine),
            Cohort::uuid("manager", other),
            Cohort::uuid("owner", Uuid::now_v7()),
            Cohort::flag("manager", true),
        ];
        let managers = Cohort::uuids(&cohorts, "manager");
        assert_eq!(managers, vec![mine, other]);
    }

    #[test]
    fn texts_extracts_only_the_named_dimensions_text_values() {
        let cohorts = vec![
            Cohort::text("plan", "pro"),
            Cohort::text("region", "eu"),
            Cohort::uuid("plan", Uuid::now_v7()),
        ];
        assert_eq!(Cohort::texts(&cohorts, "plan"), vec!["pro".to_string()]);
    }

    #[test]
    fn holds_reports_whether_a_flag_dimension_carries_the_asked_value() {
        let cohorts = vec![
            Cohort::flag("public", true),
            Cohort::uuid("org", Uuid::now_v7()),
        ];
        assert!(Cohort::holds(&cohorts, "public", true));
        assert!(!Cohort::holds(&cohorts, "public", false));
        assert!(!Cohort::holds(&cohorts, "private", true));
    }
}
