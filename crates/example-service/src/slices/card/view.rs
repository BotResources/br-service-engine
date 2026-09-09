use std::collections::BTreeMap;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
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

pub struct CardFacts {
    rows: BTreeMap<Uuid, CardState>,
}

#[derive(Serialize, Deserialize)]
pub struct BoardWindow {
    pub board_id: Uuid,
}

#[derive(Default)]
pub struct CardsView;

impl CardsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("cards");
}

impl Projector for CardsView {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = CardFacts;
    type View = CardView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Card::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        window: &'a WindowParams,
        _principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let keys = match window.get::<Uuid>("board_id") {
                Some(board) => store::cards_of_board(pg, board).await?,
                None => store::all_card_ids(pg).await?,
            };
            Ok(Population::Keys(keys.into_iter().collect()))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<CardFacts, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => store::load_cards(pg, &keys).await?,
                LoadScope::PerPrincipal { conn, .. } => store::load_cards_conn(conn, &keys).await?,
            };
            Ok(CardFacts {
                rows: rows.into_iter().map(|row| (row.id, row)).collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &CardFacts,
        key: &Uuid,
        _principal: &AppPrincipal,
    ) -> Option<CardView> {
        facts.rows.get(key).map(|state| CardView {
            id: state.id,
            board_id: state.board_id,
            title: state.title.clone(),
            status: state.status.as_str().to_string(),
        })
    }
}
