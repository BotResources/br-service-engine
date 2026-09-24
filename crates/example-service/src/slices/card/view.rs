use serde::{Deserialize, Serialize};
use service_engine::Page;
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::aggregate::Card;
use super::store::{self, CardAggregate, CardStore};
use crate::kernel::AppPrincipal;

service_engine::open_access!(
    pub CardsOpenAccess = "cards are scoped by their parent board's window, not by a cohort"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct CardView {
    pub id: Uuid,
    pub board_id: Uuid,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoardWindow {
    board_id: Option<Uuid>,
    page: Option<Page<Uuid>>,
}

impl BoardWindow {
    pub fn of(board_id: Uuid) -> Self {
        Self {
            board_id: Some(board_id),
            page: None,
        }
    }

    pub fn paged(board_id: Uuid, page: Page<Uuid>) -> Self {
        Self {
            board_id: Some(board_id),
            page: Some(page),
        }
    }
}

fn keyed(keys: Vec<Uuid>) -> Population<Uuid> {
    Population::Keys(keys.into_iter().collect())
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
    type Visibility = Unrestricted<CardAggregate, AppPrincipal, CardsOpenAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        query: &BoardWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        let pg = cx.pool();
        Ok(match (query.board_id, &query.page) {
            (Some(board), Some(page)) => {
                page.population(store::cards_of_board_page(pg, board, page).await?)
            }
            (Some(board), None) => keyed(store::cards_of_board(pg, board).await?),
            (None, _) => keyed(store::all_card_ids(pg).await?),
        })
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
