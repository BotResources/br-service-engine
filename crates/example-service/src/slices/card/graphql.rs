use async_graphql::{Context, Object, Result, SimpleObject, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::graphql::SliceFragment;
use service_engine::session::{SessionId, WindowSpec};
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
        "cardPageDeltas",
        "advanceCard",
        "scheduleCardDeadline",
        "importCards",
        "pageCards",
    ],
    types: &["CardView"],
};

#[derive(SimpleObject, Debug, Clone, Copy)]
pub struct CardPage {
    pub added: u64,
    pub released: u64,
    pub delivered: u64,
}

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

    async fn page_cards(
        &self,
        ctx: &Context<'_>,
        session: Uuid,
        board_id: Uuid,
        before: Uuid,
        size: i64,
    ) -> Result<CardPage> {
        let report = service_engine::page::<AppPrincipal, CardsView>(
            ctx,
            SessionId::from(session),
            &BoardWindow::page(board_id, before, size),
        )
        .await?;
        Ok(CardPage {
            added: report.added as u64,
            released: report.released as u64,
            delivered: report.delivered as u64,
        })
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

    async fn card_page_deltas(
        &self,
        ctx: &Context<'_>,
        session: Uuid,
        board_id: Uuid,
        size: i64,
    ) -> Result<impl Stream<Item = Result<CardDelta>>> {
        let stream = service_engine::attach_with_session::<AppPrincipal>(
            ctx,
            SessionId::from(session),
            vec![WindowSpec::view::<CardsView>(
                &BoardWindow::head(board_id, size),
                false,
            )?],
        )
        .await?;
        Ok(stream.map(|delta| CardDelta::from_delta(&delta)))
    }
}
