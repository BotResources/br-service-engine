//! The board capability: the board-scoped view of cards and the delta
//! subscriptions over one board's window.

use async_graphql::{Context, Object, Result, SimpleObject, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::{SessionId, WindowSpec};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use crate::kernel::AppPrincipal;
use crate::slices::card::mutations::ImportCards;
use crate::slices::card::view::{BoardWindow, CardView, CardsView};

service_engine::subscription_union! {
    view = CardViewUnion;
    delta = CardDelta { reset = CardReset, upsert = CardUpsert, remove = CardRemove };
    Card => service_engine::view::ViewProjector<CardsView> => CardView,
}

#[derive(SimpleObject, Debug, Clone, Copy)]
pub struct CardPage {
    pub added: u64,
    pub released: u64,
    pub delivered: u64,
}

#[derive(Default)]
pub struct CardBoardQuery;

#[Object]
impl CardBoardQuery {
    async fn cards(&self, ctx: &Context<'_>, board_id: Uuid) -> Result<Vec<CardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<CardsView>(&BoardWindow::of(board_id))
            .await
    }
}

#[derive(Default)]
pub struct CardBoardMutation;

#[Object]
impl CardBoardMutation {
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
pub struct CardBoardSubscription;

#[Subscription]
impl CardBoardSubscription {
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
