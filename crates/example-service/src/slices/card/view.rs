use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, View};
use sqlx::PgConnection;
use uuid::Uuid;

use super::aggregate::{Card, CardState};
use super::store;
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

impl View for CardsView {
    type Principal = AppPrincipal;
    type Noun = Card;
    type Row = CardState;
    type Query = BoardWindow;
    type Out = CardView;

    const NAME: ProjectorName = Self::NAME;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> service_engine::view::RowsFuture<'a, Uuid, CardState> {
        Box::pin(async move {
            Ok(store::load_cards_conn(conn, keys)
                .await?
                .into_iter()
                .map(|state| (state.id, state))
                .collect())
        })
    }

    fn populate<'a>(
        cx: &'a Populate<'a, AppPrincipal>,
        query: &'a BoardWindow,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let keys = match query.board_id {
                Some(board) => store::cards_of_board(cx.pool(), board).await?,
                None => store::all_card_ids(cx.pool()).await?,
            };
            Ok(Population::Keys(keys.into_iter().collect()))
        })
    }

    fn project(state: &CardState, _principal: &AppPrincipal) -> CardView {
        CardView {
            id: state.id,
            board_id: state.board_id,
            title: state.title.clone(),
            status: state.status.as_str().to_string(),
        }
    }
}
