use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::visibility::Visibility;
use service_engine::wire::Noun;
use std::collections::BTreeMap;
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

fn view_of(row: &BoardRow, principal: &AppPrincipal) -> BoardView {
    BoardView {
        id: row.id,
        name: row.name.clone(),
        is_public: row.is_public,
        archived: row.state == BoardState::Archived,
        affordances: row.affordances(principal),
    }
}

pub struct BoardFacts {
    rows: BTreeMap<Uuid, BoardRow>,
}

async fn load_scope(scope: LoadScope<'_, Uuid, AppPrincipal>) -> Result<BoardFacts, EngineError> {
    let keys = scope.keys().to_vec();
    let rows = match scope {
        LoadScope::Bulk { pg, .. } => store::load_boards_pool(pg, &keys).await?,
        LoadScope::PerPrincipal { conn, .. } => store::load_boards(conn, &keys).await?,
    };
    Ok(BoardFacts {
        rows: rows.into_iter().map(|row| (row.id, row)).collect(),
    })
}

#[derive(Default)]
pub struct BoardsView;

impl BoardsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("boards");
}

impl Projector for BoardsView {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = BoardFacts;
    type View = BoardView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Board::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let candidates = store::candidate_boards(pg).await?;
            Ok(Board::window(candidates, principal))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<BoardFacts, EngineError>> {
        Box::pin(load_scope(scope))
    }

    fn project(
        &self,
        facts: &BoardFacts,
        key: &Uuid,
        principal: &AppPrincipal,
    ) -> Option<BoardView> {
        facts.rows.get(key).map(|row| view_of(row, principal))
    }
}

#[derive(Default)]
pub struct OrgBoardsRls;

impl OrgBoardsRls {
    pub const NAME: ProjectorName = ProjectorName::from_static("org_boards_rls");
}

impl Projector for OrgBoardsRls {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = BoardFacts;
    type View = BoardView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Board::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        _principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let candidates = store::candidate_boards(pg).await?;
            Ok(Population::Keys(
                candidates.into_iter().map(|(id, _)| id).collect(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<BoardFacts, EngineError>> {
        Box::pin(load_scope(scope))
    }

    fn project(
        &self,
        facts: &BoardFacts,
        key: &Uuid,
        principal: &AppPrincipal,
    ) -> Option<BoardView> {
        facts.rows.get(key).map(|row| view_of(row, principal))
    }
}
