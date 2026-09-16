use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use super::mutations::RecordEntry;
use super::view::{LedgerView, LedgersView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = LedgerViewUnion;
    delta = LedgerDelta { reset = LedgerReset, upsert = LedgerUpsert, remove = LedgerRemove };
    Ledger => service_engine::view::ViewProjector<LedgersView> => LedgerView,
}

#[derive(Default)]
pub struct LedgerQuery;

#[Object]
impl LedgerQuery {
    async fn ledger(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<LedgerView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view::<LedgersView>(&id)
            .await
    }
}

#[derive(Default)]
pub struct LedgerMutation;

#[Object]
impl LedgerMutation {
    async fn record_entry(&self, ctx: &Context<'_>, id: Uuid, amount: i64) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, RecordEntry>(ctx, RecordEntry { id, amount }).await
    }
}

#[derive(Default)]
pub struct LedgerSubscription;

#[Subscription]
impl LedgerSubscription {
    async fn ledger_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<LedgerDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(
                LedgersView::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| LedgerDelta::from_delta(&delta)))
    }
}
