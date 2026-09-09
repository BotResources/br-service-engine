use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::aggregate::Card;
use super::store::{self, CardAggregate, CardStore};
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct CardView {
    pub id: Uuid,
    pub board_id: Uuid,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoardWindow {
    pub board_id: Option<Uuid>,
}

impl BoardWindow {
    pub fn of(board_id: Uuid) -> Self {
        Self {
            board_id: Some(board_id),
        }
    }
}

#[derive(Default)]
pub struct CardsView;

impl CardsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("cards");
}

impl Projector for CardsView {
    type Principal = AppPrincipal;
    type Noun = Card;
    type Store = CardStore;
    type Query = BoardWindow;
    type Out = CardView;
    type Visibility = Unrestricted<CardAggregate, AppPrincipal>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        query: &BoardWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        let keys = match query.board_id {
            Some(board) => store::cards_of_board(cx.pool(), board).await?,
            None => store::all_card_ids(cx.pool()).await?,
        };
        Ok(Population::Keys(keys.into_iter().collect()))
    }

    fn project(card: &CardAggregate, _principal: &AppPrincipal) -> CardView {
        let state = &card.0;
        CardView {
            id: state.id,
            board_id: state.board_id,
            title: state.title.clone(),
            status: state.status.as_str().to_string(),
        }
    }
}
