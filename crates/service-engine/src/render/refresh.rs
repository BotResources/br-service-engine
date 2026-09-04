use std::collections::{BTreeMap, BTreeSet};

use crate::erase::ErasedPopulation;
use crate::error::EngineError;
use crate::impact::Impact;
use crate::principal::{Principal, PrincipalId};
use crate::render::fault::Faults;
use crate::render::pass::{PassContext, PassReport, Vanished};
use crate::render::route::SessionWork;
use crate::session::SessionId;
use crate::session::live::refreshed_members;
use crate::session::store::SessionTable;
use crate::wire::KeyBytes;

pub(crate) async fn refresh_principals<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    impacts: &[Impact],
    report: &mut PassReport,
) -> Result<BTreeSet<SessionId>, EngineError> {
    let principals: BTreeSet<PrincipalId> = impacts
        .iter()
        .filter_map(|impact| match impact {
            Impact::PrincipalFactsChanged { principal, .. } => Some(*principal),
            _ => None,
        })
        .collect();
    let mut refreshed = BTreeSet::new();
    for principal in principals {
        let Some(current) = table.principal_of(principal) else {
            continue;
        };
        let resolver = ctx
            .registry
            .resolver()
            .ok_or(EngineError::MissingPrincipalResolver)?;
        match resolver.resolve(ctx.pg, &current).await {
            Err(error) => {
                tracing::error!(
                    %principal,
                    reason = %crate::chain::describe(&error),
                    "a principal refresh failed; its sessions are ended fail-closed rather than \
                     served under stale facts",
                );
                report.ended += table.end_principal(principal).len();
            }
            Ok(None) => report.ended += table.end_principal(principal).len(),
            Ok(Some(next)) => {
                table.replace_principal(principal, next);
                refreshed.extend(table.sessions_of(principal));
            }
        }
    }
    Ok(refreshed)
}

pub(crate) async fn repopulate<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    work: &mut BTreeMap<SessionId, SessionWork>,
    report: &mut PassReport,
    faults: &mut Faults,
) -> Result<Vanished, EngineError> {
    let mut vanished = Vanished::new();
    let targets: Vec<(SessionId, usize)> = work
        .iter()
        .flat_map(|(id, session_work)| {
            session_work
                .windows
                .iter()
                .filter(|(_, w)| w.repopulate || !w.discovered.is_empty())
                .map(|(index, _)| (*id, *index))
        })
        .collect();

    for (id, index) in targets {
        if faults.contains(id) {
            continue;
        }
        let Some(session) = table.get(id) else {
            continue;
        };
        let window = &session.windows[index];
        let principal = session.principal.clone();
        let params = window.params.clone();
        let name = window.projector.clone();
        let previous = window.members.clone();
        let previous_shape = window.shape.clone();
        let projector = ctx
            .registry
            .projector(&name)
            .ok_or_else(|| EngineError::UnboundProjector(name.clone()))?
            .clone();

        let entry = work
            .get_mut(&id)
            .and_then(|w| w.windows.get_mut(&index))
            .expect("the target was taken from the work map");
        let discovered = entry.discovered.clone();
        let mut next: BTreeSet<KeyBytes> = previous.clone();
        next.extend(discovered.iter().cloned());
        let mut shape = None;
        if entry.repopulate {
            let population = match projector.populate(ctx.pg, &params, &principal).await {
                Ok(population) => population,
                Err(error) => {
                    faults.record(id, &error);
                    continue;
                }
            };
            report.populates += 1;
            if let ErasedPopulation::Query(query) = &population
                && query.interest().is_empty()
            {
                faults.record(
                    id,
                    &EngineError::EmptyInterest {
                        projector: name.clone(),
                    },
                );
                continue;
            }
            next = refreshed_members(&previous, &discovered, &population);
            shape = Some(previous_shape.refreshed(&population));
        }
        for key in next.difference(&previous) {
            entry.dirty.entry(key.clone()).or_default();
        }
        entry.dirty.retain(|key, _| next.contains(key));
        let gone: Vec<KeyBytes> = previous.difference(&next).cloned().collect();
        let churn = next.difference(&previous).count() + gone.len();
        for key in &gone {
            vanished
                .entry(id)
                .or_default()
                .insert((name.clone(), key.clone()));
        }
        let Some(session) = table.get_mut(id) else {
            continue;
        };
        let window = &mut session.windows[index];
        window.members = next;
        if let Some(shape) = shape {
            window.shape = shape;
        }
        if churn > ctx.config.reset_threshold {
            session.reset_pending = true;
        }
    }
    Ok(vanished)
}
