use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::principal::RlsApplier;
use service_engine::view::{Populate, Projector, cohort_window};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::aggregate::{Board, BoardRow, BoardState};
use super::store::{self, BoardStore, OrgBoardStore};
use crate::kernel::{AppPrincipal, AppRls};

service_engine::open_access!(
    pub OrgBoardsRlsAccess = "org boards are filtered by Postgres RLS, not by an engine cohort"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct BoardView {
    pub id: Uuid,
    pub name: String,
    pub is_public: bool,
    pub archived: bool,
    pub affordances: Affordances,
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
    type Query = ();
    type Out = BoardView;
    type Visibility = Board;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        cohort_window::<Self>(cx).await
    }

    fn project(row: &BoardRow, principal: &AppPrincipal) -> Result<BoardView, EngineError> {
        Ok(view_of(row, principal))
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
    type Visibility = Unrestricted<BoardRow, AppPrincipal, OrgBoardsRlsAccess>;

    const NAME: ProjectorName = Self::NAME;

    const RLS: bool = true;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        let mut tx = cx.pool().begin().await?;
        AppRls.apply(&mut tx, cx.principal()).await?;
        let ids = store::org_board_ids(&mut *tx, cx.limit_all()).await?;
        tx.rollback().await?;
        Ok(Population::Keys(ids.into_iter().collect()))
    }

    fn project(row: &BoardRow, principal: &AppPrincipal) -> Result<BoardView, EngineError> {
        Ok(view_of(row, principal))
    }
}
