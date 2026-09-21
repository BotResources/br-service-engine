use std::collections::{BTreeMap, BTreeSet};

use async_graphql::{Context, Object, Result};
use futures_util::future::BoxFuture;
use service_engine::Query;
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentFacts, AssignmentRow, AssignmentView};
use crate::sample::principal::SamplePrincipal;

#[derive(Default)]
pub struct RlsAssignmentProjector;

impl RlsAssignmentProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("rls_assignments");
}

impl Projector for RlsAssignmentProjector {
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

    fn renders_under_rls(&self) -> bool {
        true
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id FROM sample_assignment")
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
                    .collect::<BTreeMap<_, _>>(),
            })
        })
    }

    fn project(
        &self,
        facts: &AssignmentFacts,
        key: &Uuid,
        _principal: &SamplePrincipal,
    ) -> Result<Option<AssignmentView>, EngineError> {
        let Some(row) = facts.rows.get(key) else {
            return Ok(None);
        };
        Ok(Some(AssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            can_close: !row.closed,
        }))
    }
}

#[derive(Default)]
pub struct RlsQueryRoot;

#[Object]
impl RlsQueryRoot {
    async fn sample_rls_assignment(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<AssignmentView>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch::<RlsAssignmentProjector>(&id)
            .await
    }
}
