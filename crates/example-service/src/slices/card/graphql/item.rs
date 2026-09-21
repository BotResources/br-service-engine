//! The item capability: reading and driving a single card.

use async_graphql::{Context, Object, Result};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use crate::kernel::AppPrincipal;
use crate::slices::card::mutations::{AdvanceCard, ScheduleCardDeadline};
use crate::slices::card::view::{CardView, CardsView};

#[derive(Default)]
pub struct CardItemQuery;

#[Object]
impl CardItemQuery {
    async fn example_card(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<CardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view::<CardsView>(&id)
            .await
    }
}

#[derive(Default)]
pub struct CardItemMutation;

#[Object]
impl CardItemMutation {
    async fn example_advance_card(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, AdvanceCard>(ctx, AdvanceCard { id }).await
    }

    async fn example_schedule_card_deadline(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        in_seconds: i64,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, ScheduleCardDeadline>(
            ctx,
            ScheduleCardDeadline { id, in_seconds },
        )
        .await
    }
}
