use std::collections::BTreeSet;

use crate::dyn_compat::{ErasedPopulation, ErasedWindowQuery};
use crate::name::ProjectorName;
use crate::session::WindowParams;
use crate::wire::KeyBytes;

#[derive(Debug, Clone)]
pub(crate) enum WindowShape {
    Fixed,
    Ordered { open_head: bool },
    Query(ErasedWindowQuery),
}

impl WindowShape {
    pub(crate) fn of(population: &ErasedPopulation) -> Self {
        match population {
            ErasedPopulation::Keys(_) => Self::Fixed,
            ErasedPopulation::Ordered { open_head, .. } => Self::Ordered {
                open_head: *open_head,
            },
            ErasedPopulation::Query(query) => Self::Query(query.clone()),
        }
    }

    pub(crate) fn query(&self) -> Option<&ErasedWindowQuery> {
        match self {
            Self::Query(query) => Some(query),
            _ => None,
        }
    }

    pub(crate) fn refreshed(&self, population: &ErasedPopulation) -> Self {
        match (self, population) {
            (Self::Query(_), ErasedPopulation::Query(_)) => Self::of(population),
            (Self::Query(_), _) => self.clone(),
            _ => Self::of(population),
        }
    }
}

pub(crate) fn refreshed_members(
    previous: &BTreeSet<KeyBytes>,
    discovered: &BTreeSet<KeyBytes>,
    population: &ErasedPopulation,
) -> BTreeSet<KeyBytes> {
    match population {
        ErasedPopulation::Query(query) if query.authoritative() => {
            let mut members = query.keys().clone();
            members.extend(discovered.iter().cloned());
            members
        }
        ErasedPopulation::Query(_) => {
            let mut members = previous.clone();
            members.extend(discovered.iter().cloned());
            members
        }
        keyed => members_of(keyed),
    }
}

pub(crate) fn members_of(population: &ErasedPopulation) -> BTreeSet<KeyBytes> {
    match population {
        ErasedPopulation::Keys(keys) => keys.clone(),
        ErasedPopulation::Ordered { keys, .. } => keys.iter().cloned().collect(),
        ErasedPopulation::Query(query) => query.keys().clone(),
    }
}

pub(crate) struct WindowState {
    pub(crate) projector: ProjectorName,
    pub(crate) params: WindowParams,
    pub(crate) members: BTreeSet<KeyBytes>,
    pub(crate) shape: WindowShape,
    pub(crate) pages: Vec<BTreeSet<KeyBytes>>,
}

impl WindowState {
    pub(crate) fn evict_to_capacity(&mut self, capacity: usize) -> Vec<KeyBytes> {
        let mut dropped = Vec::new();
        if self.members.len() <= capacity {
            return dropped;
        }
        let all_pages: BTreeSet<KeyBytes> = self.pages.iter().flatten().cloned().collect();
        let head: BTreeSet<KeyBytes> = self.members.difference(&all_pages).cloned().collect();
        while self.members.len() > capacity && !self.pages.is_empty() {
            let page = self.pages.remove(0);
            let retained: BTreeSet<KeyBytes> = self.pages.iter().flatten().cloned().collect();
            for key in page {
                if head.contains(&key) || retained.contains(&key) {
                    continue;
                }
                if self.members.remove(&key) {
                    dropped.push(key);
                }
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u32) -> KeyBytes {
        KeyBytes::encode(&n).expect("a key encodes")
    }

    fn windowed(members: &[u32], pages: &[&[u32]]) -> WindowState {
        WindowState {
            projector: ProjectorName::from_static("paged"),
            params: WindowParams::none(),
            members: members.iter().copied().map(key).collect(),
            shape: WindowShape::Fixed,
            pages: pages
                .iter()
                .map(|page| page.iter().copied().map(key).collect())
                .collect(),
        }
    }

    #[test]
    fn eviction_releases_the_oldest_pages_first_and_always_keeps_the_live_head() {
        let mut window = windowed(&[1, 2, 3, 4, 5, 6, 7, 8], &[&[3, 4], &[5, 6], &[7, 8]]);
        let released = window.evict_to_capacity(4);
        assert_eq!(
            released,
            vec![key(3), key(4), key(5), key(6)],
            "the two oldest pages are released, in order, to reach capacity"
        );
        let survivors: BTreeSet<KeyBytes> = [1, 2, 7, 8].into_iter().map(key).collect();
        assert_eq!(
            window.members, survivors,
            "the head (1, 2) and the newest retained page (7, 8) survive"
        );
        assert_eq!(
            window.pages.len(),
            1,
            "only the newest page is still tracked"
        );
    }

    #[test]
    fn eviction_under_capacity_releases_nothing() {
        let mut window = windowed(&[1, 2, 3, 4], &[&[3, 4]]);
        assert!(window.evict_to_capacity(4).is_empty());
        assert_eq!(window.members.len(), 4);
        assert_eq!(window.pages.len(), 1);
    }

    #[test]
    fn eviction_keeps_a_key_a_retained_page_still_holds() {
        let mut window = windowed(&[1, 2, 3, 4], &[&[3, 4], &[3]]);
        let released = window.evict_to_capacity(3);
        assert_eq!(
            released,
            vec![key(4)],
            "key 3 is retained because the newer page still holds it; only 4 is released"
        );
        assert!(window.members.contains(&key(3)));
    }
}
