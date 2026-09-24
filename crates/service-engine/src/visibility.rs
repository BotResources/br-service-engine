use std::collections::BTreeSet;
use std::marker::PhantomData;

use crate::cohort::Cohort;
use crate::impact::Deps;

pub type Cohorts = Vec<Cohort>;

pub trait Visibility: Send + Sync + 'static {
    type Row;
    type Principal;

    const DEPS: Deps = Deps::EMPTY;

    const LIVE: bool = true;

    const OPEN_ACCESS_REASON: Option<&'static str> = None;

    fn cohorts(row: &Self::Row) -> Cohorts;
    fn memberships(principal: &Self::Principal) -> Cohorts;

    fn visible(row: &Self::Row, principal: &Self::Principal) -> bool {
        let memberships: BTreeSet<Cohort> = Self::memberships(principal).into_iter().collect();
        Self::cohorts(row)
            .into_iter()
            .any(|cohort| memberships.contains(&cohort))
    }
}

pub trait AccessReason: Send + Sync + 'static {
    const REASON: &'static str;
}

type UnrestrictedMarker<Row, Principal, Why> = PhantomData<fn() -> (Row, Principal, Why)>;

pub struct Unrestricted<Row, Principal, Why: AccessReason>(UnrestrictedMarker<Row, Principal, Why>);

impl<Row, Principal, Why> Visibility for Unrestricted<Row, Principal, Why>
where
    Row: Send + Sync + 'static,
    Principal: Send + Sync + 'static,
    Why: AccessReason,
{
    type Row = Row;
    type Principal = Principal;

    const OPEN_ACCESS_REASON: Option<&'static str> = Some(Why::REASON);

    fn cohorts(_row: &Row) -> Cohorts {
        Vec::new()
    }

    fn memberships(_principal: &Principal) -> Cohorts {
        Vec::new()
    }

    fn visible(_row: &Row, _principal: &Principal) -> bool {
        true
    }
}

#[macro_export]
macro_rules! open_access {
    ($(#[$meta:meta])* $vis:vis $name:ident = $reason:literal) => {
        $(#[$meta])*
        $vis struct $name;

        impl $crate::visibility::AccessReason for $name {
            const REASON: &'static str = $reason;
        }
    };
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
    let declared: BTreeSet<K> = candidates
        .into_iter()
        .filter(|(_, row)| V::visible(row, principal))
        .map(|(key, _)| key)
        .collect();
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
