use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;

use crate::accumulator::ChunkReader;
use crate::chain::describe;
use crate::cohort::CohortKey;
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::impact::Impact;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::registry::RenderRegistry;
use crate::render::deliver::deliver;
use crate::render::fault::{Faults, SessionFault};
use crate::render::group::{Rendered, Renderer};
use crate::render::plan::{PlannedWindow, outgoing_for, plan};
use crate::render::refresh::{refresh_principals, repopulate};
use crate::render::repair::resnapshot;
use crate::render::route;
use crate::session::SessionId;
use crate::session::store::SessionTable;
use crate::wire::KeyBytes;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassReport {
    pub impacts: usize,
    pub populates: usize,
    pub loads: usize,
    pub projections: usize,
    pub cohorts: usize,
    pub deltas: usize,
    pub discarded: usize,
    pub resets: usize,
    pub sessions: usize,
    pub ended: usize,
    pub lagged: usize,
    pub faults: Vec<SessionFault>,
}

impl PassReport {
    pub fn repaired(&self) -> usize {
        self.faults.iter().filter(|fault| fault.repaired).count()
    }
}

pub(crate) struct PassContext<'a, P: Principal> {
    pub(crate) pg: &'a PgPool,
    pub(crate) registry: &'a RenderRegistry<P>,
    pub(crate) chunks: &'a ChunkReader,
    pub(crate) config: &'a EngineConfig,
}

pub(crate) type GroupKey = (ProjectorName, bool, CohortKey);
pub(crate) type Vanished = BTreeMap<SessionId, BTreeSet<(ProjectorName, KeyBytes)>>;

struct Group<P> {
    keys: BTreeSet<KeyBytes>,
    representative: P,
}

pub(crate) async fn run_pass_focused<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    impacts: &[Impact],
    focus: Option<SessionId>,
) -> Result<PassReport, EngineError> {
    let mut report = PassReport {
        impacts: impacts.len(),
        ..PassReport::default()
    };
    let mut faults = Faults::default();
    if focus.is_none() {
        table.hold_for_pending(impacts, ctx.config.max_held_impacts);
        for id in table
            .iter()
            .filter(|session| session.is_live() && session.repair_pending)
            .map(|session| session.id)
            .collect::<Vec<_>>()
        {
            faults.mark(id, "retrying a repair that has not yet succeeded");
        }
    }

    let refreshed = refresh_principals(ctx, table, impacts, &mut report).await?;
    let inverses = route::resolve_inverses(impacts, ctx.registry);
    let mut work = route::route(
        impacts,
        ctx.registry,
        table,
        &refreshed,
        &inverses,
        &mut faults,
        focus,
    );
    let vanished = repopulate(ctx, table, &mut work, &mut report, &mut faults).await?;
    work.retain(|id, _| !faults.contains(*id));
    let plans = plan(ctx, table, work)?;

    let mut groups: BTreeMap<GroupKey, Group<P>> = BTreeMap::new();
    for windows in plans.values() {
        for window in windows {
            let entry = groups.entry(window.group()).or_insert_with(|| Group {
                keys: BTreeSet::new(),
                representative: window.representative.clone(),
            });
            entry.keys.extend(window.dirty.keys().cloned());
        }
    }
    report.cohorts = groups.len();

    let renderer = Renderer {
        pg: ctx.pg,
        chunks: ctx.chunks,
        rls: ctx.registry.rls(),
    };
    let mut rendered: BTreeMap<GroupKey, Rendered> = BTreeMap::new();
    for (group_key, group) in &groups {
        let projector = ctx
            .registry
            .projector(&group_key.0)
            .ok_or_else(|| EngineError::UnboundProjector(group_key.0.clone()))?;
        let keys: Vec<KeyBytes> = group.keys.iter().cloned().collect();
        match renderer
            .render(
                projector,
                group_key.1,
                group_key.2.clone(),
                &group.representative,
                &keys,
            )
            .await
        {
            Ok((views, cost)) => {
                report.loads += cost.loads;
                report.projections += cost.projections;
                rendered.insert(group_key.clone(), views);
            }
            Err(error) => {
                for (id, windows) in &plans {
                    if windows.iter().any(|window| &window.group() == group_key) {
                        faults.record(*id, &error);
                    }
                }
            }
        }
    }

    let delivery = Delivery {
        plans: &plans,
        rendered: &rendered,
        vanished: &vanished,
        impacts,
    };
    deliver_pass(ctx, table, &delivery, &mut report, &mut faults, focus);
    repair_faulted(ctx, table, faults, &mut report).await;
    Ok(report)
}

struct Delivery<'a, P: Principal> {
    plans: &'a BTreeMap<SessionId, Vec<PlannedWindow<P>>>,
    rendered: &'a BTreeMap<GroupKey, Rendered>,
    vanished: &'a Vanished,
    impacts: &'a [Impact],
}

fn deliver_pass<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    delivery: &Delivery<'_, P>,
    report: &mut PassReport,
    faults: &mut Faults,
    focus: Option<SessionId>,
) {
    let Delivery {
        plans,
        rendered,
        vanished,
        impacts,
    } = delivery;
    let mut targets: BTreeSet<SessionId> = plans.keys().copied().collect();
    targets.extend(vanished.keys().copied());
    if focus.is_none() {
        targets.extend(
            table
                .iter()
                .filter(|session| session.is_live() && session.reset_pending)
                .map(|session| session.id),
        );
    }
    let nothing: Vec<PlannedWindow<P>> = Vec::new();
    for id in targets {
        if faults.contains(id) {
            continue;
        }
        let windows = plans.get(&id).unwrap_or(&nothing);
        let outgoing = match outgoing_for(ctx, table, id, windows, rendered, vanished, impacts) {
            Ok(outgoing) => outgoing,
            Err(error) => {
                faults.record(id, &error);
                continue;
            }
        };
        let Some(session) = table.get_mut(id) else {
            continue;
        };
        if outgoing.is_empty() && !session.reset_pending {
            continue;
        }
        let delivered = deliver(session, outgoing);
        report.deltas += delivered.deltas;
        report.discarded += delivered.discarded;
        report.resets += delivered.resets;
        report.lagged += usize::from(delivered.lagged);
        report.sessions += 1;
    }
}

async fn repair_faulted<P: Principal>(
    ctx: &PassContext<'_, P>,
    table: &mut SessionTable<P>,
    faults: Faults,
    report: &mut PassReport,
) {
    if faults.is_empty() {
        return;
    }
    let mut repaired = BTreeSet::new();
    for id in faults.sessions() {
        match resnapshot(ctx, table, id).await {
            Ok(cost) => {
                report.populates += cost.populates;
                report.loads += cost.loads;
                report.projections += cost.projections;
                report.resets += 1;
                if let Some(session) = table.get_mut(id) {
                    session.repair_pending = false;
                    session.repair_attempts = 0;
                }
                repaired.insert(id);
            }
            Err(error) => {
                let Some(session) = table.get_mut(id) else {
                    continue;
                };
                session.repair_attempts += 1;
                let attempts = session.repair_attempts;
                if attempts >= ctx.config.repair_attempts {
                    tracing::error!(
                        session = %id,
                        attempts,
                        reason = %describe(&error),
                        "a session could not be re-snapshotted after repeated attempts, so it is \
                         ended rather than served a Reset built from its stale last-sent view"
                    );
                    session.end();
                    report.ended += 1;
                } else {
                    session.repair_pending = true;
                    tracing::warn!(
                        session = %id,
                        attempts,
                        reason = %describe(&error),
                        "a faulted session could not be re-snapshotted; it holds its last good \
                         view and the next pass retries the repair, never a Reset from stale state"
                    );
                }
            }
        }
    }
    report.faults = faults.into_report(&repaired);
}

#[cfg(test)]
mod tests;
