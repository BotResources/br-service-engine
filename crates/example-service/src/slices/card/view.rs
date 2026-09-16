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
    pub before: Option<Uuid>,
    pub size: Option<i64>,
}

impl BoardWindow {
    pub fn of(board_id: Uuid) -> Self {
        Self {
            board_id: Some(board_id),
            before: None,
            size: None,
        }
    }

    pub fn head(board_id: Uuid, size: i64) -> Self {
        Self {
            board_id: Some(board_id),
            before: None,
            size: Some(size),
        }
    }

    pub fn page(board_id: Uuid, before: Uuid, size: i64) -> Self {
        Self {
            board_id: Some(board_id),
            before: Some(before),
            size: Some(size),
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
        let keys = match (query.board_id, query.before, query.size) {
            (Some(board), Some(before), Some(size)) => {
                store::cards_of_board_before(cx.pool(), board, before, size).await?
            }
            (Some(board), None, Some(size)) => {
                store::cards_of_board_head(cx.pool(), board, size).await?
            }
            (Some(board), _, None) => store::cards_of_board(cx.pool(), board).await?,
            (None, _, _) => store::all_card_ids(cx.pool()).await?,
        };
        Ok(Population::Keys(keys.into_iter().collect()))
    }

    fn project(card: &CardAggregate, _principal: &AppPrincipal) -> Result<CardView, EngineError> {
        let state = &card.0;
        Ok(CardView {
            id: state.id,
            board_id: state.board_id,
            title: state.title.clone(),
            status: state.status.as_str().to_string(),
        })
    }
}
