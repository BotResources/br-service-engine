use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::aggregate::Ledger;
use super::store::{self, LedgerAggregate, LedgerStore};
use crate::kernel::AppPrincipal;

service_engine::open_access!(
    /// The ledger is a per-service running total with no per-viewer scoping:
    /// every authenticated viewer sees the same aggregate.
    pub LedgersOpenAccess = "the ledger total is a service-wide aggregate open to every viewer"
);

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
    type Visibility = Unrestricted<LedgerAggregate, AppPrincipal, LedgersOpenAccess>;

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
