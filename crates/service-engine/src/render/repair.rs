use std::collections::BTreeSet;

use crate::cohort::CohortKey;
use crate::erase::ErasedPopulation;
use crate::error::EngineError;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::render::group::{Rendered, Renderer};
use crate::render::pass::PassContext;
use crate::session::live::{WindowShape, refreshed_members};
use crate::session::store::SessionTable;
use crate::session::{SessionId, WindowParams};
use crate::wire::KeyBytes;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RepairCost {
    pub(crate) populates: usize,
    pub(crate) loads: usize,
    pub(crate) projections: usize,
}

struct WindowSpecShot {
    name: ProjectorName,
    params: WindowParams,
    rls: bool,
    members: BTreeSet<KeyBytes>,
    shape: WindowShape,
}

struct WindowShot {
    members: BTreeSet<KeyBytes>,
    shape: WindowShape,
    views: Rendered,
}

pub(crate) async fn resnapshot<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    id: SessionId,
) -> Result<RepairCost, EngineError> {
    let Some(session) = table.get(id) else {
        return Ok(RepairCost::default());
    };
    let principal = session.principal.clone();
    let specs: Vec<WindowSpecShot> = session
        .windows
        .iter()
        .map(|window| WindowSpecShot {
            name: window.projector.clone(),
            params: window.params.clone(),
            rls: window.rls,
            members: window.members.clone(),
            shape: window.shape.clone(),
        })
        .collect();

    let renderer = Renderer {
        pg: ctx.pg,
        chunks: ctx.chunks,
        rls: ctx.registry.rls(),
    };
    let mut cost = RepairCost::default();
    let mut shots = Vec::with_capacity(specs.len());
    for spec in &specs {
        let name = &spec.name;
        let projector = ctx
            .registry
            .projector(name)
            .ok_or_else(|| EngineError::UnboundProjector(name.clone()))?;
        let population = projector.populate(ctx.pg, &spec.params, &principal).await?;
        cost.populates += 1;
        if let ErasedPopulation::Query(query) = &population
            && query.interest().is_empty()
        {
            return Err(EngineError::EmptyInterest {
                projector: name.clone(),
            });
        }
        let members = refreshed_members(&spec.members, &BTreeSet::new(), &population);
        let shape = spec.shape.refreshed(&population);
        let cohort = if spec.rls {
            CohortKey::principal(principal.id())
        } else {
            projector.cohort(&principal)
        };
        let keys: Vec<KeyBytes> = members.iter().cloned().collect();
        let (views, rendered) = renderer
            .render(projector, spec.rls, cohort, &principal, &keys)
            .await?;
        cost.loads += rendered.loads;
        cost.projections += rendered.projections;
        shots.push(WindowShot {
            members,
            shape,
            views,
        });
    }

    let Some(session) = table.get_mut(id) else {
        return Ok(cost);
    };
    let mut seeded = Vec::new();
    for (index, shot) in shots.into_iter().enumerate() {
        let window = &mut session.windows[index];
        window.members = shot.members;
        window.shape = shot.shape;
        let projector = window.projector.clone();
        for (key, view) in shot.views {
            if let Some(view) = view {
                seeded.push(((projector.clone(), key), view));
            }
        }
    }
    session.last_sent.clear();
    session.last_sent.extend(seeded);
    session.reset_now();
    Ok(cost)
}
