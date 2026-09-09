use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::wire::Noun;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

pub struct Reply;

impl Noun for Reply {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("reply");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyRow {
    pub id: Uuid,
    pub board_id: Uuid,
    pub text: String,
    pub status: String,
    pub blob_ref: Option<Uuid>,
}

fn row_to_reply(row: &sqlx::postgres::PgRow) -> ReplyRow {
    ReplyRow {
        id: row.get("id"),
        board_id: row.get("board_id"),
        text: row.get("text"),
        status: row.get("status"),
        blob_ref: row.get("blob_ref"),
    }
}

pub struct ReplyStore;

impl Persistence for ReplyStore {
    type Aggregate = ReplyRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<ReplyRow>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, board_id, text, status, blob_ref FROM reply WHERE id = $1 FOR UPDATE",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.as_ref().map(row_to_reply))
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        reply: &'a ReplyRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO reply (id, board_id, text, status, blob_ref) VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (id) DO UPDATE SET text = EXCLUDED.text, status = EXCLUDED.status, \
                   blob_ref = EXCLUDED.blob_ref",
            )
            .bind(reply.id)
            .bind(reply.board_id)
            .bind(&reply.text)
            .bind(&reply.status)
            .bind(reply.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        reply: &'a ReplyRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO reply (id, board_id, text, status, blob_ref) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(reply.id)
            .bind(reply.board_id)
            .bind(&reply.text)
            .bind(&reply.status)
            .bind(reply.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

impl Aggregate for ReplyRow {
    type Store = ReplyStore;

    fn key(&self) -> Uuid {
        self.id
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        self.blob_ref.map(BlobRef).into_iter().collect()
    }
}

pub async fn all_reply_ids(pg: &PgPool) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM reply").fetch_all(pg).await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn load_replies(pg: &PgPool, keys: &[Uuid]) -> Result<Vec<ReplyRow>, EngineError> {
    let rows =
        sqlx::query("SELECT id, board_id, text, status, blob_ref FROM reply WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(pg)
            .await?;
    Ok(rows.iter().map(row_to_reply).collect())
}

pub async fn load_replies_conn(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<ReplyRow>, EngineError> {
    let rows =
        sqlx::query("SELECT id, board_id, text, status, blob_ref FROM reply WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(conn)
            .await?;
    Ok(rows.iter().map(row_to_reply).collect())
}
