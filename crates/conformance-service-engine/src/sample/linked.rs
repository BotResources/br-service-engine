use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use service_engine::KeyCeiling;
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

pub const LINK_NAMESPACE: &str = "catalog.entry";

pub async fn link_assignment(pool: &PgPool, assignment_id: Uuid, foreign_key: &str) {
    sqlx::query(
        "INSERT INTO sample_assignment_link (assignment_id, foreign_key) VALUES ($1, $2) \
         ON CONFLICT DO NOTHING",
    )
    .bind(assignment_id)
    .bind(foreign_key)
    .execute(pool)
    .await
    .expect("link the sample assignment to a foreign key");
}

#[derive(Default)]
pub struct LinkedAssignments;

impl LinkedAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("linked_assignments");
}

impl Projector for LinkedAssignments {
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
                rows.iter().map(|row| row.get::<Uuid, _>("id")).collect(),
            ))
        })
    }

    fn inverse(&self, foreign: &ForeignKey) -> Inverse<Uuid> {
        if foreign.namespace().as_str() != LINK_NAMESPACE {
            return Inverse::None;
        }
        Inverse::Lookup(Arc::new(|conn, foreign: &ForeignKey| {
            let foreign_key = foreign.key().as_str().to_string();
            Box::pin(async move {
                let rows = sqlx::query(
                    "SELECT assignment_id FROM sample_assignment_link WHERE foreign_key = $1",
                )
                .bind(&foreign_key)
                .fetch_all(conn)
                .await?;
                Ok(rows
                    .iter()
                    .map(|row| row.get::<Uuid, _>("assignment_id"))
                    .collect())
            })
        }))
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
            let rows: BTreeMap<Uuid, AssignmentRow> = rows
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
                .collect();
            Ok(AssignmentFacts { rows })
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
