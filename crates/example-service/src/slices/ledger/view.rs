use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use uuid::Uuid;

use super::aggregate::Ledger;
use super::store::{self, LedgerAggregate, LedgerStore};
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct LedgerView {
    pub id: Uuid,
    pub total: i64,
    pub last_author: Option<Uuid>,
}

#[derive(Default)]
pub struct LedgersView;

impl LedgersView {
    pub const NAME: ProjectorName = ProjectorName::from_static("ledgers");
}

impl Projector for LedgersView {
    type Principal = AppPrincipal;
    type Noun = Ledger;
    type Store = LedgerStore;
    type Query = ();
    type Out = LedgerView;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            store::all_ledger_ids(cx.pool())
                .await?
                .into_iter()
                .collect(),
        ))
    }

    fn project(ledger: &LedgerAggregate, _principal: &AppPrincipal) -> LedgerView {
        let state = &ledger.0;
        LedgerView {
            id: state.id,
            total: state.total,
            last_author: state.last_author,
        }
    }
}
