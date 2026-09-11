use example_contract::{BOARD_PREFIX, PublishedBoard};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::nats::KvKey;
use service_engine::offer::Offer;
use sqlx::PgConnection;
use uuid::Uuid;

use super::aggregate::{BoardRow, BoardState};
use super::store;

pub struct BoardOffer;

pub fn board_key(id: Uuid) -> Result<KvKey, EngineError> {
    KvKey::new(format!("{BOARD_PREFIX}{id}"))
        .map_err(|error| EngineError::Config(format!("board offer key: {error}")))
}

impl Offer for BoardOffer {
    type Row = BoardRow;
    type Published = PublishedBoard;

    const NAME: &'static str = "board_v1";
    const PREFIX: &'static str = BOARD_PREFIX;

    fn key(row: &BoardRow) -> Result<KvKey, EngineError> {
        board_key(row.id)
    }

    fn publish(row: &BoardRow) -> Option<PublishedBoard> {
        (row.state == BoardState::Active).then(|| PublishedBoard {
            id: row.id,
            org_id: row.org_id,
            name: row.name.clone(),
        })
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<BoardRow>, EngineError>> {
        Box::pin(async move { store::all_boards(conn).await })
    }
}
