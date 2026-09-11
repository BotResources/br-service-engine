use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::principal::RlsApplier;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::{Unrestricted, Visibility};
use uuid::Uuid;

use super::aggregate::{Board, BoardRow, BoardState};
use super::store::{self, BoardStore, OrgBoardStore};
use crate::kernel::{AppPrincipal, AppRls};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct BoardView {
    pub id: Uuid,
    pub name: String,
    pub is_public: bool,
    pub archived: bool,
    pub affordances: Affordances,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoardFilter {
    pub public_only: bool,
}

fn view_of(row: &BoardRow, principal: &AppPrincipal) -> BoardView {
    BoardView {
        id: row.id,
        name: row.name.clone(),
        is_public: row.is_public,
        archived: row.state == BoardState::Archived,
        affordances: row.affordances(principal),
    }
}

#[derive(Default)]
pub struct BoardsView;

impl BoardsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("boards");
}

impl Projector for BoardsView {
    type Principal = AppPrincipal;
    type Noun = Board;
    type Store = BoardStore;
    type Query = BoardFilter;
    type Out = BoardView;
    type Visibility = Board;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        query: &BoardFilter,
    ) -> Result<Population<Uuid>, EngineError> {
        let mut candidates = store::candidate_boards(cx.pool()).await?;
        if query.public_only {
            candidates.retain(|(_, row)| row.is_public);
        }
        Ok(Board::window(candidates, cx.principal()))
    }

    fn project(row: &BoardRow, principal: &AppPrincipal) -> BoardView {
        view_of(row, principal)
    }
}

#[derive(Default)]
pub struct OrgBoardsRls;

impl OrgBoardsRls {
    pub const NAME: ProjectorName = ProjectorName::from_static("org_boards_rls");
}

impl Projector for OrgBoardsRls {
    type Principal = AppPrincipal;
    type Noun = Board;
    type Store = OrgBoardStore;
    type Query = ();
    type Out = BoardView;
    type Visibility = Unrestricted<BoardRow, AppPrincipal>;

    const NAME: ProjectorName = Self::NAME;

    const RLS: bool = true;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        let mut tx = cx.pool().begin().await?;
        AppRls.apply(&mut tx, cx.principal()).await?;
        let ids = store::org_board_ids(&mut *tx).await?;
        tx.rollback().await?;
        Ok(Population::Keys(ids.into_iter().collect()))
    }

    fn project(row: &BoardRow, principal: &AppPrincipal) -> BoardView {
        view_of(row, principal)
    }
}
