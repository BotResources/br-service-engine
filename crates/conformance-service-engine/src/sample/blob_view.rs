use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::impact::{ForeignKey, Impact};
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{Emission, LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::Row;
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub struct Doc;

impl Noun for Doc {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("doc");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocFacts {
    pub rows: BTreeMap<Uuid, DocRowView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocRowView {
    pub id: Uuid,
    pub name: String,
    pub blob_ref: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocView {
    pub id: Uuid,
    pub name: String,
    pub blob_ref: Option<Uuid>,
}

pub struct DocProjector;

impl DocProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("docs");
}

impl Projector for DocProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = DocFacts;
    type View = DocView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Doc::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        ceiling: KeyCeiling,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id FROM sample_doc WHERE tenant_id = $1 LIMIT $2")
                .bind(principal.tenant())
                .bind(ceiling.limit())
                .fetch_all(pg)
                .await?;
            Ok(Population::Keys(
                rows.iter()
                    .map(|row| row.get::<Uuid, _>("id"))
                    .collect::<BTreeSet<_>>(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn emission(&self, _impact: &Impact) -> Emission {
        Emission::PerImpact
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<DocFacts, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT id, name, blob_ref FROM sample_doc WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(DocFacts {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let id: Uuid = row.get("id");
                        (
                            id,
                            DocRowView {
                                id,
                                name: row.get("name"),
                                blob_ref: row.get("blob_ref"),
                            },
                        )
                    })
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &DocFacts,
        key: &Uuid,
        _principal: &SamplePrincipal,
    ) -> Result<Option<DocView>, EngineError> {
        let Some(row) = facts.rows.get(key) else {
            return Ok(None);
        };
        Ok(Some(DocView {
            id: row.id,
            name: row.name.clone(),
            blob_ref: row.blob_ref,
        }))
    }
}
