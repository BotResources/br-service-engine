use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, View};
use service_engine::visibility::Visibility;
use sqlx::PgConnection;
use uuid::Uuid;

use super::aggregate::{Board, BoardRow, BoardState};
use super::store;
use crate::kernel::AppPrincipal;

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

fn load_rows<'a>(
    conn: &'a mut PgConnection,
    keys: &'a [Uuid],
) -> service_engine::view::RowsFuture<'a, Uuid, BoardRow> {
    Box::pin(async move {
        Ok(store::load_boards(conn, keys)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect())
    })
}

#[derive(Default)]
pub struct BoardsView;

impl BoardsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("boards");
}

impl View for BoardsView {
    type Principal = AppPrincipal;
    type Noun = Board;
    type Row = BoardRow;
    type Query = BoardFilter;
    type Out = BoardView;

    const NAME: ProjectorName = Self::NAME;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> service_engine::view::RowsFuture<'a, Uuid, BoardRow> {
        load_rows(conn, keys)
    }

    fn populate<'a>(
        cx: &'a Populate<'a, AppPrincipal>,
        query: &'a BoardFilter,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let mut candidates = store::candidate_boards(cx.pool()).await?;
            if query.public_only {
                candidates.retain(|(_, row)| row.is_public);
            }
            Ok(Board::window(candidates, cx.principal()))
        })
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

impl View for OrgBoardsRls {
    type Principal = AppPrincipal;
    type Noun = Board;
    type Row = BoardRow;
    type Query = ();
    type Out = BoardView;

    const NAME: ProjectorName = Self::NAME;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> service_engine::view::RowsFuture<'a, Uuid, BoardRow> {
        load_rows(conn, keys)
    }

    fn populate<'a>(
        cx: &'a Populate<'a, AppPrincipal>,
        _query: &'a (),
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let candidates = store::candidate_boards(cx.pool()).await?;
            Ok(Population::Keys(
                candidates.into_iter().map(|(id, _)| id).collect(),
            ))
        })
    }

    fn project(row: &BoardRow, principal: &AppPrincipal) -> BoardView {
        view_of(row, principal)
    }
}
