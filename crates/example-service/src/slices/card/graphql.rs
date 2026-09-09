use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::graphql::SliceFragment;
use service_engine::session::WindowSpec;
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use super::mutations::{AdvanceCard, ImportCards, ScheduleCardDeadline};
use super::view::{BoardWindow, CardView, CardsView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = CardViewUnion;
    delta = CardDelta { reset = CardReset, upsert = CardUpsert, remove = CardRemove };
    Card => service_engine::view::ViewProjector<CardsView> => CardView,
}

pub const FRAGMENT: SliceFragment = SliceFragment {
    slice: "card",
    root_fields: &[
        "card",
        "cards",
        "cardDeltas",
        "advanceCard",
        "scheduleCardDeadline",
        "importCards",
    ],
    types: &["CardView"],
};

#[derive(Default)]
pub struct CardQuery;

#[Object]
impl CardQuery {
    async fn card(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<CardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view::<CardsView>(&id)
            .await
    }

    async fn cards(&self, ctx: &Context<'_>, board_id: Uuid) -> Result<Vec<CardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<CardsView>(&BoardWindow::of(board_id))
            .await
    }
}

#[derive(Default)]
pub struct CardMutation;

#[Object]
impl CardMutation {
    async fn advance_card(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, AdvanceCard>(ctx, AdvanceCard { id }).await
    }

    async fn schedule_card_deadline(
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

    async fn import_cards(
        &self,
        ctx: &Context<'_>,
        board_id: Uuid,
        titles: Vec<String>,
    ) -> Result<MutationAck> {
        service_engine::ack_bulk::<AppPrincipal, ImportCards>(ctx, ImportCards { board_id, titles })
            .await
    }
}

#[derive(Default)]
pub struct CardSubscription;

#[Subscription]
impl CardSubscription {
    async fn card_deltas(
        &self,
        ctx: &Context<'_>,
        board_id: Uuid,
    ) -> Result<impl Stream<Item = Result<CardDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::view::<CardsView>(
                &BoardWindow::of(board_id),
                false,
            )?],
        )
        .await?;
        Ok(stream.map(|delta| CardDelta::from_delta(&delta)))
    }
}
