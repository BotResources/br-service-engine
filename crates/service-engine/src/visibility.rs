use std::collections::BTreeSet;
use std::marker::PhantomData;

use crate::cohort::CohortKey;
use crate::population::Population;

pub type Cohorts = Vec<CohortKey>;

pub trait Visibility: Send + Sync + 'static {
    type Row;
    type Principal;

    fn cohorts(row: &Self::Row) -> Cohorts;
    fn memberships(principal: &Self::Principal) -> Cohorts;

    fn visible(row: &Self::Row, principal: &Self::Principal) -> bool {
        let memberships: BTreeSet<CohortKey> = Self::memberships(principal).into_iter().collect();
        Self::cohorts(row)
            .into_iter()
            .any(|cohort| memberships.contains(&cohort))
    }

    fn visible_keys<K, I>(candidates: I, principal: &Self::Principal) -> BTreeSet<K>
    where
        K: Ord,
        I: IntoIterator<Item = (K, Self::Row)>,
    {
        let memberships: BTreeSet<CohortKey> = Self::memberships(principal).into_iter().collect();
        candidates
            .into_iter()
            .filter(|(_, row)| {
                Self::cohorts(row)
                    .into_iter()
                    .any(|cohort| memberships.contains(&cohort))
            })
            .map(|(key, _)| key)
            .collect()
    }

    fn window<K, I>(candidates: I, principal: &Self::Principal) -> Population<K>
    where
        K: Ord,
        I: IntoIterator<Item = (K, Self::Row)>,
    {
        Population::Keys(Self::visible_keys(candidates, principal))
    }
}

pub struct Unrestricted<Row, Principal>(PhantomData<fn() -> (Row, Principal)>);

impl<Row, Principal> Visibility for Unrestricted<Row, Principal>
where
    Row: Send + Sync + 'static,
    Principal: Send + Sync + 'static,
{
    type Row = Row;
    type Principal = Principal;

    fn cohorts(_row: &Row) -> Cohorts {
        Vec::new()
    }

    fn memberships(_principal: &Principal) -> Cohorts {
        Vec::new()
    }

    fn visible(_row: &Row, _principal: &Principal) -> bool {
        true
    }

    fn visible_keys<K, I>(candidates: I, _principal: &Principal) -> BTreeSet<K>
    where
        K: Ord,
        I: IntoIterator<Item = (K, Row)>,
    {
        candidates.into_iter().map(|(key, _)| key).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowMismatch<K> {
    pub only_in_window: Vec<K>,
    pub only_in_declaration: Vec<K>,
}

impl<K: std::fmt::Debug> std::fmt::Display for WindowMismatch<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the session window and the visibility declaration diverge: \
             only in the window {:?}, only in the declaration {:?}",
            self.only_in_window, self.only_in_declaration
        )
    }
}

impl<K: std::fmt::Debug> std::error::Error for WindowMismatch<K> {}

pub fn check_window_matches_visibility<V, K, I>(
    window: &BTreeSet<K>,
    candidates: I,
    principal: &V::Principal,
) -> Result<(), WindowMismatch<K>>
where
    V: Visibility,
    K: Ord + Clone,
    I: IntoIterator<Item = (K, V::Row)>,
{
    let declared = V::visible_keys::<K, _>(candidates, principal);
    let only_in_window: Vec<K> = window.difference(&declared).cloned().collect();
    let only_in_declaration: Vec<K> = declared.difference(window).cloned().collect();
    if only_in_window.is_empty() && only_in_declaration.is_empty() {
        Ok(())
    } else {
        Err(WindowMismatch {
            only_in_window,
            only_in_declaration,
        })
    }
}

#[cfg(test)]
mod tests;
