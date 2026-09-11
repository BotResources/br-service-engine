use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::Assignment;
use crate::sample::principal::SamplePrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleView {
    pub id: Uuid,
    pub title: String,
}

#[derive(Debug, Default)]
pub struct TitleFacts {
    pub titles: BTreeMap<Uuid, String>,
}

pub struct TitleProjector {
    renders: Arc<AtomicUsize>,
}

impl TitleProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("assignment_titles");

    pub fn new(renders: Arc<AtomicUsize>) -> Self {
        Self { renders }
    }
}

impl Projector for TitleProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = TitleFacts;
    type View = TitleView;

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
        _principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id FROM sample_assignment")
                .fetch_all(pg)
                .await?;
            Ok(Population::Keys(
                rows.iter().map(|r| r.get::<Uuid, _>("id")).collect(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<TitleFacts, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT id, title FROM sample_assignment WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(TitleFacts {
                titles: rows
                    .iter()
                    .map(|row| (row.get::<Uuid, _>("id"), row.get::<String, _>("title")))
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &TitleFacts,
        key: &Uuid,
        _principal: &SamplePrincipal,
    ) -> Option<TitleView> {
        self.renders.fetch_add(1, Ordering::Relaxed);
        facts.titles.get(key).map(|title| TitleView {
            id: *key,
            title: title.clone(),
        })
    }
}

pub struct MiskeyedProjector;

impl MiskeyedProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("miskeyed_assignments");
}

impl Projector for MiskeyedProjector {
    type Principal = SamplePrincipal;
    type Key = String;
    type Facts = ();
    type View = TitleView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Assignment::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<String>, EngineError>> {
        Box::pin(async move { Ok(Population::Keys(BTreeSet::new())) })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<String> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        _scope: LoadScope<'a, String, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move { Ok(()) })
    }

    fn project(
        &self,
        _facts: &(),
        _key: &String,
        _principal: &SamplePrincipal,
    ) -> Option<TitleView> {
        None
    }
}
