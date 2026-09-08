use std::collections::BTreeSet;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Serialize;
use service_engine::CohortKey;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gate, Gated, Reason};
use service_engine::impact::{Deps, Dims, ForeignKey, Impact};
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Interest, Inverse, Population, WindowQuery};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentFacts, AssignmentRow};
use crate::sample::principal::SamplePrincipal;

pub const DEP_MEMBERSHIP: u8 = 0;

pub mod reasons {
    use service_engine::gate::Reason;

    pub const ALREADY_CLOSED: Reason = Reason::new("already_closed");
    pub const NOT_CLOSED: Reason = Reason::new("not_closed");
}

service_engine::gated! {
    AssignmentRow, SamplePrincipal;
    "close" => fn close_gate(this, _principal) {
        if this.closed {
            Gate::blocked(reasons::ALREADY_CLOSED)
        } else {
            Gate::allowed()
        }
    }
    "reopen" => fn reopen_gate(this, _principal) {
        if this.closed {
            Gate::allowed()
        } else {
            Gate::blocked(reasons::NOT_CLOSED)
        }
    }
}

impl AssignmentRow {
    pub fn close(&mut self, principal: &SamplePrincipal) -> Result<(), Reason> {
        self.close_gate(principal).require()?;
        self.closed = true;
        Ok(())
    }

    pub fn reopen(&mut self, principal: &SamplePrincipal) -> Result<(), Reason> {
        self.reopen_gate(principal).require()?;
        self.closed = false;
        Ok(())
    }
}

pub struct AssignmentVisibility;

impl Visibility for AssignmentVisibility {
    type Row = AssignmentRow;
    type Principal = SamplePrincipal;

    fn cohorts(row: &AssignmentRow) -> Cohorts {
        vec![CohortKey::of(&[row.tenant_id])]
    }

    fn memberships(principal: &SamplePrincipal) -> Cohorts {
        vec![CohortKey::of(&[principal.tenant()])]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatedAssignmentView {
    pub id: Uuid,
    pub title: String,
    pub closed: bool,
    pub affordances: Affordances,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Keys,
    All,
    Membership,
}

pub struct GatedAssignmentProjector {
    mode: Mode,
}

impl GatedAssignmentProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("gated_assignments");

    pub fn keys() -> Self {
        Self { mode: Mode::Keys }
    }

    pub fn all() -> Self {
        Self { mode: Mode::All }
    }

    pub fn membership() -> Self {
        Self {
            mode: Mode::Membership,
        }
    }

    async fn all_keys(pg: &PgPool) -> Result<Vec<Uuid>, EngineError> {
        let rows = sqlx::query("SELECT id FROM sample_assignment")
            .fetch_all(pg)
            .await?;
        Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
    }
}

pub async fn load_candidates(pg: &PgPool) -> Result<Vec<(Uuid, AssignmentRow)>, EngineError> {
    let rows = sqlx::query("SELECT id, tenant_id, title, closed FROM sample_assignment")
        .fetch_all(pg)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let id: Uuid = row.get("id");
            (
                id,
                AssignmentRow {
                    id,
                    tenant_id: row.get("tenant_id"),
                    title: row.get("title"),
                    closed: row.get("closed"),
                },
            )
        })
        .collect())
}

fn membership_dep() -> Deps {
    Deps::bit(DEP_MEMBERSHIP).expect("a declared dependency fits the bit set")
}

impl Projector for GatedAssignmentProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = AssignmentFacts;
    type View = GatedAssignmentView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Assignment::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            match self.mode {
                Mode::Keys => {
                    let candidates = load_candidates(pg).await?;
                    Ok(AssignmentVisibility::window(candidates, principal))
                }
                Mode::All => {
                    let keys = Self::all_keys(pg).await?;
                    Ok(Population::Keys(keys.into_iter().collect::<BTreeSet<_>>()))
                }
                Mode::Membership => {
                    let candidates = load_candidates(pg).await?;
                    let keys = AssignmentVisibility::visible_keys(candidates, principal);
                    let interest = Interest::new()
                        .on_noun(Assignment::NAME, Dims::EMPTY)
                        .on_deps(membership_dep());
                    Ok(Population::Query(
                        WindowQuery::new(interest, Arc::new(|_: &Uuid, _: &Impact| false))
                            .with_keys(keys),
                    ))
                }
            }
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<AssignmentFacts, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT id, tenant_id, title, closed FROM sample_assignment \
                       WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(AssignmentFacts {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let id: Uuid = row.get("id");
                        (
                            id,
                            AssignmentRow {
                                id,
                                tenant_id: row.get("tenant_id"),
                                title: row.get("title"),
                                closed: row.get("closed"),
                            },
                        )
                    })
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &AssignmentFacts,
        key: &Uuid,
        principal: &SamplePrincipal,
    ) -> Option<GatedAssignmentView> {
        let row = facts.rows.get(key)?;
        if !AssignmentVisibility::visible(row, principal) {
            return None;
        }
        Some(GatedAssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            affordances: row.affordances(principal),
        })
    }
}
