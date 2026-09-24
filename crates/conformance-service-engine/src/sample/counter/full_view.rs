use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::Row;
use uuid::Uuid;

use crate::sample::counter::domain::{CounterState, CounterView, view_of};
use crate::sample::counter::full::FullCounterNoun;
use crate::sample::principal::SamplePrincipal;

pub struct FullCounterFacts {
    pub rows: BTreeMap<Uuid, CounterState>,
}

pub struct FullCounterProjector;

impl FullCounterProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("counters_full");
}

impl Projector for FullCounterProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = FullCounterFacts;
    type View = CounterView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[FullCounterNoun::NAME];
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
            let rows = sqlx::query(
                "SELECT id FROM sample_counter_full_snapshot WHERE tenant = $1 LIMIT $2",
            )
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

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<FullCounterFacts, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT id, tenant, total, closed, last_author, version \
                       FROM sample_counter_full_snapshot WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(FullCounterFacts {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let id: Uuid = row.get("id");
                        (
                            id,
                            CounterState::from_row(
                                id,
                                row.get("tenant"),
                                row.get("total"),
                                row.get("closed"),
                                row.get("last_author"),
                                row.get("version"),
                            ),
                        )
                    })
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &FullCounterFacts,
        key: &Uuid,
        principal: &SamplePrincipal,
    ) -> Result<Option<CounterView>, EngineError> {
        Ok(facts.rows.get(key).map(|state| view_of(state, principal)))
    }
}
