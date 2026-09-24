use std::collections::BTreeSet;

use crate::dyn_compat::{ErasedPopulation, ErasedWindowQuery};
use crate::name::ProjectorName;
use crate::session::capacity::note_growth;
use crate::session::{SessionId, WindowParams};
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
}

impl WindowState {
    pub(crate) fn replace_members(
        &mut self,
        members: BTreeSet<KeyBytes>,
        session: SessionId,
        capacity: usize,
    ) {
        note_growth(
            session,
            &self.projector,
            self.members.len(),
            members.len(),
            capacity,
        );
        self.members = members;
    }
}
