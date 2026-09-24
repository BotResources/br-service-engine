use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::Cohort;
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::impact::{Dims, ForeignKey};
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub const DIM_TITLE: u8 = 0;
pub const DIM_LIFECYCLE: u8 = 1;

pub struct Assignment;

impl Noun for Assignment {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("assignment");
}

pub fn title_dim() -> Dims {
    Dims::bit(DIM_TITLE).expect("a declared dimension fits the bit set")
}

pub fn lifecycle_dim() -> Dims {
    Dims::bit(DIM_LIFECYCLE).expect("a declared dimension fits the bit set")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentFacts {
    pub rows: BTreeMap<Uuid, AssignmentRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub title: String,
    pub closed: bool,
}

pub struct AssignmentStore;

impl Persistence for AssignmentStore {
    type Aggregate = AssignmentRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<AssignmentRow>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, tenant_id, title, closed FROM sample_assignment WHERE id = $1",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.map(|row| AssignmentRow {
                id: row.get("id"),
                tenant_id: row.get("tenant_id"),
                title: row.get("title"),
                closed: row.get("closed"),
            }))
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, AssignmentRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, tenant_id, title, closed FROM sample_assignment WHERE id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    let assignment = AssignmentRow {
                        id: row.get("id"),
                        tenant_id: row.get("tenant_id"),
                        title: row.get("title"),
                        closed: row.get("closed"),
                    };
                    (assignment.id, assignment)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a AssignmentRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_assignment (id, tenant_id, title, closed) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET \
                   tenant_id = EXCLUDED.tenant_id, title = EXCLUDED.title, closed = EXCLUDED.closed",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.title)
            .bind(aggregate.closed)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a AssignmentRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_assignment (id, tenant_id, title, closed) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.title)
            .bind(aggregate.closed)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

impl Aggregate for AssignmentRow {
    type Store = AssignmentStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

impl CohortIndex for AssignmentStore {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        Box::pin(async move {
            let tenants = Cohort::uuids(cohorts, "tenant");
            let rows = sqlx::query("SELECT id FROM sample_assignment WHERE tenant_id = ANY($1)")
                .bind(&tenants)
                .fetch_all(conn)
                .await?;
            Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct AssignmentView {
    pub id: Uuid,
    pub title: String,
    pub closed: bool,
    pub can_close: bool,
}

#[derive(Default)]
pub struct AssignmentProjector;

impl AssignmentProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("assignments");
}

impl Projector for AssignmentProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = AssignmentFacts;
    type View = AssignmentView;

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
        _ceiling: KeyCeiling,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id FROM sample_assignment WHERE tenant_id = $1")
                .bind(principal.tenant())
                .fetch_all(pg)
                .await?;
            Ok(Population::Keys(
                rows.iter()
                    .map(|r| r.get::<Uuid, _>("id"))
                    .collect::<BTreeSet<_>>(),
            ))
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
    ) -> Result<Option<AssignmentView>, EngineError> {
        let Some(row) = facts.rows.get(key) else {
            return Ok(None);
        };
        if row.tenant_id != principal.tenant() {
            return Ok(None);
        }
        Ok(Some(AssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            can_close: !row.closed,
        }))
    }
}
