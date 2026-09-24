use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::WindowSpec;
use service_engine::{MutationAck, Page, Query, WindowSize};
use uuid::Uuid;

use crate::kernel::AppPrincipal;
use crate::slices::card::mutations::ImportCards;
use crate::slices::card::view::{BoardWindow, CardView, CardsView};

service_engine::subscription_union! {
    view = CardViewUnion;
    delta = CardDelta { reset = CardReset, upsert = CardUpsert, remove = CardRemove };
    Card => service_engine::view::ViewProjector<CardsView> => CardView,
}

#[derive(Default)]
pub struct CardBoardQuery;

#[Object]
impl CardBoardQuery {
    async fn example_cards(&self, ctx: &Context<'_>, board_id: Uuid) -> Result<Vec<CardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<CardsView>(&BoardWindow::of(board_id))
            .await
    }
}

#[derive(Default)]
pub struct CardBoardMutation;

#[Object]
impl CardBoardMutation {
    async fn example_import_cards(
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
pub struct CardBoardSubscription;

#[Subscription]
impl CardBoardSubscription {
    async fn example_card_deltas(
        &self,
        ctx: &Context<'_>,
        board_id: Uuid,
        size: WindowSize,
        before: Option<Uuid>,
    ) -> Result<impl Stream<Item = Result<CardDelta>>> {
        let window = BoardWindow::paged(board_id, Page::new(before, size));
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::view::<CardsView>(&window, false)?],
        )
        .await?;
        Ok(stream.map(|delta| CardDelta::from_delta(&delta)))
    }
}
