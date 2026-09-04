use std::collections::{BTreeMap, BTreeSet};

use crate::erase::ErasedInverse;
use crate::error::EngineError;
use crate::impact::Impact;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::registry::RenderRegistry;
use crate::render::fault::Faults;
use crate::session::SessionId;
use crate::session::live::{Session, WindowShape};
use crate::session::store::SessionTable;
use crate::wire::KeyBytes;

#[derive(Debug, Default)]
pub(crate) struct WindowWork {
    pub(crate) repopulate: bool,
    pub(crate) dirty: BTreeMap<KeyBytes, Vec<usize>>,
    pub(crate) discovered: BTreeSet<KeyBytes>,
}

impl WindowWork {
    fn touch(&mut self, key: &KeyBytes, impact: usize) {
        self.dirty.entry(key.clone()).or_default().push(impact);
    }

    pub(crate) fn is_empty(&self) -> bool {
        !self.repopulate && self.dirty.is_empty() && self.discovered.is_empty()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SessionWork {
    pub(crate) windows: BTreeMap<usize, WindowWork>,
}

impl SessionWork {
    fn window(&mut self, index: usize) -> &mut WindowWork {
        self.windows.entry(index).or_default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.windows.values().all(WindowWork::is_empty)
    }
}

#[derive(Debug, Default)]
pub(crate) struct Inverses {
    resolved: BTreeMap<(usize, ProjectorName), ErasedInverse>,
    blind: BTreeMap<ProjectorName, String>,
}

impl Inverses {
    fn get(&self, index: usize, projector: &ProjectorName) -> Option<&ErasedInverse> {
        self.resolved.get(&(index, projector.clone()))
    }

    fn blindness(&self, projector: &ProjectorName) -> Option<&str> {
        self.blind.get(projector).map(String::as_str)
    }
}

pub(crate) fn resolve_inverses<P: Principal>(
    impacts: &[Impact],
    registry: &RenderRegistry<P>,
) -> Inverses {
    let mut inverses = Inverses::default();
    for (index, impact) in impacts.iter().enumerate() {
        let Impact::ForeignChanged { foreign } = impact else {
            continue;
        };
        for (name, projector) in registry.all() {
            match projector.inverse(foreign) {
                Ok(inverse) => {
                    inverses.resolved.insert((index, name.clone()), inverse);
                }
                Err(error) => {
                    inverses
                        .blind
                        .insert(name.clone(), crate::chain::describe(&error));
                }
            }
        }
    }
    inverses
}

pub(crate) fn route<P: Principal>(
    impacts: &[Impact],
    registry: &RenderRegistry<P>,
    table: &SessionTable<P>,
    refreshed: &BTreeSet<SessionId>,
    inverses: &Inverses,
    faults: &mut Faults,
    focus: Option<SessionId>,
) -> BTreeMap<SessionId, SessionWork> {
    let mut routed: BTreeMap<SessionId, SessionWork> = BTreeMap::new();
    for session in table
        .iter()
        .filter(|s| s.is_live() && focus.is_none_or(|only| s.id == only))
    {
        if faults.contains(session.id) {
            continue;
        }
        let mut work = SessionWork::default();
        let mut faulted = None;
        for (index, impact) in impacts.iter().enumerate() {
            if let Err(error) = route_impact(session, index, impact, registry, inverses, &mut work)
            {
                faulted = Some(error);
                break;
            }
        }
        if let Some(error) = faulted {
            faults.record(session.id, &error);
            continue;
        }
        if refreshed.contains(&session.id) {
            for (index, window) in session.windows.iter().enumerate() {
                let entry = work.window(index);
                for key in &window.members {
                    entry.dirty.entry(key.clone()).or_default();
                }
                if matches!(window.shape, WindowShape::Ordered { open_head: true }) {
                    entry.repopulate = true;
                }
            }
        }
        if !work.is_empty() {
            routed.insert(session.id, work);
        }
    }
    routed
}

fn route_impact<P: Principal>(
    session: &Session<P>,
    index: usize,
    impact: &Impact,
    registry: &RenderRegistry<P>,
    inverses: &Inverses,
    work: &mut SessionWork,
) -> Result<(), EngineError> {
    match impact {
        Impact::ResourceChanged { noun, key, .. } => {
            let rendering: BTreeSet<&ProjectorName> = registry.on_noun(noun).iter().collect();
            for window_index in 0..session.windows.len() {
                let window = &session.windows[window_index];
                let renders = rendering.contains(&window.projector);
                let query = window.shape.query();
                let interested = query.is_some_and(|q| q.interest().intersects(impact));
                let member = renders && window.members.contains(key);
                let head =
                    renders && matches!(window.shape, WindowShape::Ordered { open_head: true });
                if !(member || head || interested) {
                    continue;
                }
                let discovered = match query {
                    Some(q) if interested && renders => (q.predicate())(key, impact)?,
                    _ => false,
                };
                let entry = work.window(window_index);
                if member || discovered {
                    entry.touch(key, index);
                }
                if head || interested {
                    entry.repopulate = true;
                }
                if discovered {
                    entry.discovered.insert(key.clone());
                }
            }
        }
        Impact::ForeignChanged { .. } => {
            for window_index in 0..session.windows.len() {
                let window = &session.windows[window_index];
                if let Some(reason) = inverses.blindness(&window.projector) {
                    return Err(EngineError::Service(reason.to_string().into()));
                }
                let Some(inverse) = inverses.get(index, &window.projector) else {
                    continue;
                };
                let entry = work.window(window_index);
                match inverse {
                    ErasedInverse::Keys(keys) => {
                        for key in keys.intersection(&window.members) {
                            entry.touch(key, index);
                        }
                    }
                    ErasedInverse::Query(query) => {
                        for key in &window.members {
                            if (query.predicate())(key, impact)? {
                                entry.touch(key, index);
                            }
                        }
                    }
                    ErasedInverse::None => {}
                }
                if let Some(query) = window.shape.query()
                    && query.interest().intersects(impact)
                {
                    entry.repopulate = true;
                }
            }
        }
        Impact::PrincipalFactsChanged { principal, .. } => {
            if session.principal.id() != *principal {
                return Ok(());
            }
            for window_index in 0..session.windows.len() {
                let window = &session.windows[window_index];
                let entry = work.window(window_index);
                for key in &window.members {
                    entry.touch(key, index);
                }
                match &window.shape {
                    WindowShape::Ordered { open_head: true } => entry.repopulate = true,
                    WindowShape::Query(query) if query.interest().intersects(impact) => {
                        entry.repopulate = true;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
