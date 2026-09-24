use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::aggregate::{BoardRow, BoardState};

fn state_text(state: BoardState) -> &'static str {
    match state {
        BoardState::Active => "active",
        BoardState::Archived => "archived",
    }
}

fn state_of(text: &str) -> BoardState {
    match text {
        "archived" => BoardState::Archived,
        _ => BoardState::Active,
    }
}

fn row_to_board(row: &sqlx::postgres::PgRow) -> BoardRow {
    BoardRow {
        id: row.get("id"),
        org_id: row.get("org_id"),
        name: row.get("name"),
        is_public: row.get("is_public"),
        state: state_of(row.get::<String, _>("state").as_str()),
    }
}

pub struct BoardStore;

impl Persistence for BoardStore {
    type Aggregate = BoardRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "board", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, BoardRow)>, EngineError>> {
        Box::pin(async move {
            Ok(load_boards(conn, keys)
                .await?
                .into_iter()
                .map(|row| (row.id, row))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        board: &'a BoardRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO board (id, org_id, name, is_public, state) VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, \
                   is_public = EXCLUDED.is_public, state = EXCLUDED.state",
            )
            .bind(board.id)
            .bind(board.org_id)
            .bind(&board.name)
            .bind(board.is_public)
            .bind(state_text(board.state))
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        board: &'a BoardRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO board (id, org_id, name, is_public, state) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(board.id)
            .bind(board.org_id)
            .bind(&board.name)
            .bind(board.is_public)
            .bind(state_text(board.state))
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

impl Aggregate for BoardRow {
    type Store = BoardStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

pub async fn boards_of(pg: &PgPool, user: Uuid) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT board_id FROM board_member WHERE user_id = $1")
        .bind(user)
        .fetch_all(pg)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<Uuid, _>("board_id"))
        .collect())
}

pub async fn candidate_boards<'e, E>(exec: E) -> Result<Vec<(Uuid, BoardRow)>, EngineError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query("SELECT id, org_id, name, is_public, state FROM board")
        .fetch_all(exec)
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let board = row_to_board(row);
            (board.id, board)
        })
        .collect())
}

pub async fn org_board_ids<'e, E>(exec: E) -> Result<Vec<Uuid>, EngineError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query("SELECT id FROM org_board")
        .fetch_all(exec)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub struct OrgBoardStore;

impl Persistence for OrgBoardStore {
    type Aggregate = BoardRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, BoardRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, org_id, name, is_public, state FROM org_board WHERE id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .iter()
                .map(|row| {
                    let board = row_to_board(row);
                    (board.id, board)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        board: &'a BoardRow,
        events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        BoardStore::save(conn, board, events)
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        board: &'a BoardRow,
        events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        BoardStore::create(conn, board, events)
    }
}

pub async fn load_boards(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<BoardRow>, EngineError> {
    let rows =
        sqlx::query("SELECT id, org_id, name, is_public, state FROM board WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(conn)
            .await?;
    Ok(rows.iter().map(row_to_board).collect())
}

pub async fn all_boards(conn: &mut PgConnection) -> Result<Vec<BoardRow>, EngineError> {
    let rows = sqlx::query("SELECT id, org_id, name, is_public, state FROM board")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(row_to_board).collect())
}

pub async fn add_member(
    conn: &mut PgConnection,
    board: Uuid,
    user: Uuid,
) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO board_member (board_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(board)
    .bind(user)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn remove_member(
    conn: &mut PgConnection,
    board: Uuid,
    user: Uuid,
) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM board_member WHERE board_id = $1 AND user_id = $2")
        .bind(board)
        .bind(user)
        .execute(conn)
        .await?;
    Ok(())
}
