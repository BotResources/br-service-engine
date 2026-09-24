use std::collections::BTreeSet;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Serialize;
use service_engine::Cohort;
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gate, Gated, Reason};
use service_engine::impact::{Deps, Dims, ForeignKey, Impact};
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::CohortIndex;
use service_engine::population::{Interest, Inverse, Population, WindowQuery};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentFacts, AssignmentRow, AssignmentStore};
use crate::sample::principal::SamplePrincipal;

pub const DEP_MEMBERSHIP: u8 = 0;

pub mod reasons {
    use service_engine::gate::Reason;

    pub const ALREADY_CLOSED: Reason = Reason::new("ALREADY_CLOSED");
    pub const NOT_CLOSED: Reason = Reason::new("NOT_CLOSED");
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

    const DEPS: Deps = Deps::from_bits(1 << DEP_MEMBERSHIP);

    fn cohorts(row: &AssignmentRow) -> Cohorts {
        vec![Cohort::uuid("tenant", row.tenant_id)]
    }

    fn memberships(principal: &SamplePrincipal) -> Cohorts {
        vec![Cohort::uuid("tenant", principal.tenant())]
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

    async fn tenant_keys(
        pg: &PgPool,
        principal: &SamplePrincipal,
        ceiling: KeyCeiling,
    ) -> Result<BTreeSet<Uuid>, EngineError> {
        let mut conn = pg.acquire().await?;
        let memberships = AssignmentVisibility::memberships(principal);
        let keys = AssignmentStore::keys_in_cohorts(&mut conn, &memberships, ceiling).await?;
        Ok(keys.into_iter().collect())
    }
}

pub async fn all_assignment_keys(pg: &PgPool, limit: i64) -> Result<BTreeSet<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM sample_assignment LIMIT $1")
        .bind(limit)
        .fetch_all(pg)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
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
        ceiling: KeyCeiling,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            match self.mode {
                Mode::Keys => Ok(Population::Keys(
                    Self::tenant_keys(pg, principal, ceiling).await?,
                )),
                Mode::All => Ok(Population::Keys(
                    all_assignment_keys(pg, ceiling.limit()).await?,
                )),
                Mode::Membership => {
                    let keys = Self::tenant_keys(pg, principal, ceiling).await?;
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
    ) -> Result<Option<GatedAssignmentView>, EngineError> {
        let Some(row) = facts.rows.get(key) else {
            return Ok(None);
        };
        if !AssignmentVisibility::visible(row, principal) {
            return Ok(None);
        }
        Ok(Some(GatedAssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            affordances: row.affordances(principal),
        }))
    }
}
