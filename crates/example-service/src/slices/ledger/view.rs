use std::collections::BTreeMap;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use uuid::Uuid;

use super::aggregate::Ledger;
use super::store;
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct LedgerView {
    pub id: Uuid,
    pub total: i64,
    pub last_author: Option<Uuid>,
}

pub struct LedgerFacts {
    rows: BTreeMap<Uuid, (i64, Option<Uuid>)>,
}

#[derive(Default)]
pub struct LedgersView;

impl LedgersView {
    pub const NAME: ProjectorName = ProjectorName::from_static("ledgers");
}

impl Projector for LedgersView {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = LedgerFacts;
    type View = LedgerView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Ledger::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        _principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Keys(
                store::all_ledger_ids(pg).await?.into_iter().collect(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<LedgerFacts, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let mut rows = BTreeMap::new();
            match scope {
                LoadScope::Bulk { pg, .. } => {
                    for key in keys {
                        if let Some(snap) = store::snapshot_of(pg, key).await? {
                            rows.insert(key, snap);
                        }
                    }
                }
                LoadScope::PerPrincipal { conn, .. } => {
                    for key in keys {
                        if let Some(snap) = store::snapshot_of_conn(conn, key).await? {
                            rows.insert(key, snap);
                        }
                    }
                }
            }
            Ok(LedgerFacts { rows })
        })
    }

    fn project(
        &self,
        facts: &LedgerFacts,
        key: &Uuid,
        _principal: &AppPrincipal,
    ) -> Option<LedgerView> {
        facts.rows.get(key).map(|(total, author)| LedgerView {
            id: *key,
            total: *total,
            last_author: *author,
        })
    }
}
