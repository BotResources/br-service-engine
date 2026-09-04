use std::collections::BTreeMap;

use crate::cohort::CohortKey;
use crate::error::EngineError;
use crate::impact::Impact;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::projector::Emission;
use crate::render::deliver::Outgoing;
use crate::render::diff::transition;
use crate::render::group::Rendered;
use crate::render::pass::{GroupKey, PassContext, Vanished};
use crate::render::route::SessionWork;
use crate::session::SessionId;
use crate::session::store::SessionTable;
use crate::wire::{KeyBytes, ViewBytes};

pub(crate) struct PlannedWindow<P> {
    pub(crate) representative: P,
    pub(crate) projector: ProjectorName,
    pub(crate) rls: bool,
    pub(crate) cohort: CohortKey,
    pub(crate) dirty: BTreeMap<KeyBytes, Vec<usize>>,
}

impl<P> PlannedWindow<P> {
    pub(crate) fn group(&self) -> GroupKey {
        (self.projector.clone(), self.rls, self.cohort.clone())
    }
}

pub(crate) fn plan<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &SessionTable<P>,
    work: BTreeMap<SessionId, SessionWork>,
) -> Result<BTreeMap<SessionId, Vec<PlannedWindow<P>>>, EngineError> {
    let mut planned: BTreeMap<SessionId, Vec<PlannedWindow<P>>> = BTreeMap::new();
    for (id, session_work) in work {
        let Some(session) = table.get(id) else {
            continue;
        };
        for (index, window_work) in session_work.windows {
            if window_work.dirty.is_empty() {
                continue;
            }
            let window = &session.windows[index];
            let projector = ctx
                .registry
                .projector(&window.projector)
                .ok_or_else(|| EngineError::UnboundProjector(window.projector.clone()))?;
            let cohort = if window.rls {
                CohortKey::principal(session.principal.id())
            } else {
                projector.cohort(&session.principal)
            };
            planned.entry(id).or_default().push(PlannedWindow {
                representative: session.principal.clone(),
                projector: window.projector.clone(),
                rls: window.rls,
                cohort,
                dirty: window_work.dirty,
            });
        }
    }
    Ok(planned)
}

pub(crate) fn outgoing_for<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &SessionTable<P>,
    id: SessionId,
    windows: &[PlannedWindow<P>],
    rendered: &BTreeMap<GroupKey, Rendered>,
    vanished: &Vanished,
    impacts: &[Impact],
) -> Result<Vec<Outgoing>, EngineError> {
    let Some(session) = table.get(id) else {
        return Ok(Vec::new());
    };
    let mut merged: BTreeMap<(ProjectorName, KeyBytes), Option<ViewBytes>> = BTreeMap::new();
    let mut touching: BTreeMap<(ProjectorName, KeyBytes), Vec<usize>> = BTreeMap::new();
    for window in windows {
        let views = rendered.get(&window.group());
        for (key, impact_indices) in &window.dirty {
            let entry = (window.projector.clone(), key.clone());
            let view = views.and_then(|v| v.get(key).cloned()).flatten();
            let slot = merged.entry(entry.clone()).or_insert(None);
            if slot.is_none() {
                *slot = view;
            }
            touching
                .entry(entry)
                .or_default()
                .extend(impact_indices.iter().copied());
        }
    }
    for (projector, key) in vanished.get(&id).into_iter().flatten() {
        if session.holds(projector, key) {
            continue;
        }
        merged
            .entry((projector.clone(), key.clone()))
            .or_insert(None);
    }

    let mut outgoing = Vec::new();
    for ((projector, key), next) in merged {
        let last = session.last_sent.get(&(projector.clone(), key.clone()));
        let erased = ctx
            .registry
            .projector(&projector)
            .ok_or_else(|| EngineError::UnboundProjector(projector.clone()))?;
        let per_impact: Vec<usize> = touching
            .get(&(projector.clone(), key.clone()))
            .into_iter()
            .flatten()
            .copied()
            .filter(|index| erased.emission(&impacts[*index]) == Emission::PerImpact)
            .collect();
        if per_impact.is_empty() {
            let step = transition(last, next.as_ref());
            if step.emits_upsert() {
                outgoing.push(Outgoing::Upsert {
                    projector,
                    key,
                    view: next.expect("an upsert carries the view it rendered"),
                    cause: None,
                });
            } else if step.emits_remove() {
                outgoing.push(Outgoing::Remove {
                    projector,
                    key,
                    cause: None,
                });
            }
            continue;
        }
        for index in per_impact {
            let cause =
                impacts[index]
                    .cause()
                    .cloned()
                    .ok_or_else(|| EngineError::CauseRequired {
                        projector: projector.clone(),
                    })?;
            outgoing.push(match &next {
                Some(view) => Outgoing::Upsert {
                    projector: projector.clone(),
                    key: key.clone(),
                    view: view.clone(),
                    cause: Some(cause),
                },
                None => Outgoing::Remove {
                    projector: projector.clone(),
                    key: key.clone(),
                    cause: Some(cause),
                },
            });
        }
    }
    Ok(outgoing)
}
