use std::collections::BTreeSet;
use std::marker::PhantomData;

use crate::cohort::CohortKey;
use crate::impact::Deps;
use crate::population::Population;

pub type Cohorts = Vec<CohortKey>;

pub trait Visibility: Send + Sync + 'static {
    type Row;
    type Principal;

    /// The principal-fact dependency bits that `memberships` reads.
    ///
    /// A membership is granted or revoked by staging an
    /// `Ops::impact_principal_facts` carrying these bits. A **live** window
    /// (see [`Visibility::LIVE`]) declares an `Interest` on exactly these deps,
    /// so the engine repopulates it — and only it — when those facts change.
    /// A window whose `memberships` reads no principal facts (it keys on a
    /// column that is fixed for the session, such as the caller's own org)
    /// leaves this `EMPTY`.
    const DEPS: Deps = Deps::EMPTY;

    /// Whether the visible set is **open**: rows may enter or leave the window
    /// while a session is live — a row created in one of the caller's cohorts,
    /// or a membership change. The read-side default is `true`, because a
    /// cohort view is a live surface: a newly created in-cohort row must reach
    /// an open session, and the `Population::Keys` shape does not deliver it
    /// (a fresh key is not yet a member of any window). The engine derives a
    /// `Population::Query` for a live view (see `view::windowed`); set this to
    /// `false` for a genuinely closed snapshot, the explicit override.
    const LIVE: bool = true;

    /// `Some(reason)` when this visibility deliberately applies **no** cohort
    /// gate (see [`Unrestricted`]); `None` for a real cohort declaration.
    /// `register_view` refuses a view whose visibility opts out with an empty
    /// reason, so an opt-out is always justified in the source and visible in
    /// review.
    const OPEN_ACCESS_REASON: Option<&'static str> = None;

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

    /// The in-memory windowing path: filter pre-loaded `candidates` by
    /// visibility and hand back a `Population::Keys` (a `Fixed` window).
    ///
    /// This is the fallback for a store that cannot answer a cohort query with
    /// a single indexed read. When the store implements
    /// [`crate::persistence::CohortIndex`], prefer `view::cohort_window`, which
    /// reads **only** the caller's rows through the store seam and derives the
    /// window shape from this declaration; and `view::windowed`, which turns a
    /// key set into the inferred shape. This method always yields `Keys`
    /// because it has no `Noun` to seed a live `Interest` with.
    fn window<K, I>(candidates: I, principal: &Self::Principal) -> Population<K>
    where
        K: Ord,
        I: IntoIterator<Item = (K, Self::Row)>,
    {
        Population::Keys(Self::visible_keys(candidates, principal))
    }
}

/// A named justification for a view that applies **no** cohort gate.
///
/// Implement it — or use the [`open_access!`](crate::open_access) macro — and
/// pass the marker as the third type parameter of [`Unrestricted`]. The reason
/// is checked non-empty when the view registers, so opting out of visibility
/// is a deliberate, reviewable statement rather than a silent default.
pub trait AccessReason: Send + Sync + 'static {
    /// Why this view needs no cohort gate — e.g. it filters through Postgres
    /// RLS instead, or it is genuinely open to every viewer. Must be non-empty.
    const REASON: &'static str;
}

/// A [`Visibility`] that admits every row, carrying an explicit
/// [`AccessReason`] for why no cohort gate applies. Reach for it only when a
/// view filters through another mechanism (Postgres RLS) or is open to all;
/// the reason is surfaced through [`Visibility::OPEN_ACCESS_REASON`] and
/// enforced non-empty at registration.
type UnrestrictedMarker<Row, Principal, Why> = PhantomData<fn() -> (Row, Principal, Why)>;

pub struct Unrestricted<Row, Principal, Why: AccessReason>(
    UnrestrictedMarker<Row, Principal, Why>,
);

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

    fn visible_keys<K, I>(candidates: I, _principal: &Principal) -> BTreeSet<K>
    where
        K: Ord,
        I: IntoIterator<Item = (K, Row)>,
    {
        candidates.into_iter().map(|(key, _)| key).collect()
    }
}

/// Define a zero-sized [`AccessReason`] marker in one line.
///
/// ```ignore
/// service_engine::open_access!(pub CardsOpenAccess = "cards are gated by their parent board's view");
/// type Visibility = Unrestricted<CardAggregate, AppPrincipal, CardsOpenAccess>;
/// ```
///
/// The marker's visibility must be at least that of the view that names it (a
/// projector's `type Visibility` is a public associated type), so declare it
/// `pub` (or `pub(crate)`).
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
