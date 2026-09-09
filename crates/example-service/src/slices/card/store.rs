use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::aggregate::{CardEvent, CardState, Status};

pub const EVENT_VERSION: i32 = 1;

pub struct CardAggregate(pub CardState);

fn row_to_card(row: &sqlx::postgres::PgRow) -> CardState {
    CardState::from_row(
        row.get("id"),
        row.get("board_id"),
        row.get("title"),
        Status::of(row.get::<String, _>("status").as_str()),
        row.get("version"),
    )
}

pub struct CardStore;

impl Persistence for CardStore {
    type Aggregate = CardAggregate;
    type Key = Uuid;
    type Event = CardEvent;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<CardAggregate>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, board_id, title, status, version FROM card WHERE id = $1 FOR UPDATE",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.as_ref().map(|row| CardAggregate(row_to_card(row))))
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, CardAggregate)>, EngineError>> {
        Box::pin(async move {
            Ok(load_cards_conn(conn, keys)
                .await?
                .into_iter()
                .map(|state| (state.id, CardAggregate(state)))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        card: &'a CardAggregate,
        events: &'a [CardEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, &card.0).await?;
            append_facts(conn, &card.0, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        card: &'a CardAggregate,
        events: &'a [CardEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO card (id, board_id, title, status, version) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(card.0.id)
            .bind(card.0.board_id)
            .bind(&card.0.title)
            .bind(card.0.status.as_str())
            .bind(card.0.version)
            .execute(&mut *conn)
            .await?;
            append_facts(conn, &card.0, events).await
        })
    }
}

async fn upsert(conn: &mut PgConnection, state: &CardState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO card (id, board_id, title, status, version) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status, \
           version = EXCLUDED.version",
    )
    .bind(state.id)
    .bind(state.board_id)
    .bind(&state.title)
    .bind(state.status.as_str())
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

async fn append_facts(
    conn: &mut PgConnection,
    state: &CardState,
    events: &[CardEvent],
) -> Result<(), EngineError> {
    let base = state.base_version();
    for (offset, event) in events.iter().enumerate() {
        let seq = base + offset as i64 + 1;
        let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
            what: "card fact",
            source,
        })?;
        sqlx::query("INSERT INTO card_fact (card, seq, version, payload) VALUES ($1, $2, $3, $4)")
            .bind(state.id)
            .bind(seq)
            .bind(EVENT_VERSION)
            .bind(&payload)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

impl Aggregate for CardAggregate {
    type Store = CardStore;

    fn key(&self) -> Uuid {
        self.0.id
    }

    fn pending_events(&self) -> &[CardEvent] {
        self.0.pending()
    }
}

pub async fn cards_of_board(pg: &PgPool, board: Uuid) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM card WHERE board_id = $1 ORDER BY id")
        .bind(board)
        .fetch_all(pg)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn all_card_ids(pg: &PgPool) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM card").fetch_all(pg).await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn load_cards_conn(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<CardState>, EngineError> {
    let rows =
        sqlx::query("SELECT id, board_id, title, status, version FROM card WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(conn)
            .await?;
    Ok(rows.iter().map(row_to_card).collect())
}

pub async fn purge_done(pg: &PgPool) -> Result<u64, EngineError> {
    let deleted = sqlx::query("DELETE FROM card WHERE status = 'done'")
        .execute(pg)
        .await?
        .rows_affected();
    Ok(deleted)
}
