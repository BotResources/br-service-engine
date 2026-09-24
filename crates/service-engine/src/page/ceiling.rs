#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyCeiling(usize);

impl KeyCeiling {
    pub const NONE: Self = Self(usize::MAX);

    pub(crate) fn attach(window_capacity: usize) -> Self {
        Self(window_capacity.saturating_add(1))
    }

    pub fn get(self) -> usize {
        self.0
    }

    pub fn limit(self) -> i64 {
        i64::try_from(self.0).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_attach_reads_one_key_past_window_capacity_and_no_further() {
        assert_eq!(KeyCeiling::attach(4).get(), 5);
        assert_eq!(KeyCeiling::attach(4).limit(), 5);
    }

    #[test]
    fn no_ceiling_binds_the_largest_limit_postgres_accepts() {
        assert_eq!(KeyCeiling::NONE.limit(), i64::MAX);
        assert_eq!(KeyCeiling::attach(usize::MAX), KeyCeiling::NONE);
    }
}
