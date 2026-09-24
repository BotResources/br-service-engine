use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Gate, Reason};
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::kernel::AppPrincipal;

pub const NOT_STREAMING: Reason = Reason::new("REPLY_NOT_STREAMING");

pub struct Reply;

impl Noun for Reply {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("reply");
}

impl Visibility for Reply {
    type Row = ReplyRow;
    type Principal = AppPrincipal;

    fn cohorts(row: &ReplyRow) -> Cohorts {
        vec![Cohort::uuid("org", row.org_id)]
    }

    fn memberships(principal: &AppPrincipal) -> Cohorts {
        vec![Cohort::uuid("org", principal.org())]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ReplyCause {
    Started,
    Completed { chars: usize },
    Attached,
    AttachmentUploaded,
    CancelRequested,
    Cancelled { chars: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyRow {
    pub id: Uuid,
    pub board_id: Uuid,
    pub org_id: Uuid,
    pub text: String,
    pub status: String,
    pub blob_ref: Option<Uuid>,
}

impl ReplyRow {
    pub fn open(id: Uuid, board_id: Uuid, org_id: Uuid) -> Self {
        Self {
            id,
            board_id,
            org_id,
            text: String::new(),
            status: "streaming".to_string(),
            blob_ref: None,
        }
    }

    pub fn complete(&mut self, text: String) -> ReplyCause {
        let chars = text.chars().count();
        self.text = text;
        self.status = "complete".to_string();
        ReplyCause::Completed { chars }
    }

    pub fn attach(&mut self, blob: Uuid) -> ReplyCause {
        self.blob_ref = Some(blob);
        ReplyCause::Attached
    }

    pub fn request_cancel(&mut self, principal: &AppPrincipal) -> Result<ReplyCause, Reason> {
        self.cancel_gate(principal).require()?;
        self.status = "cancelling".to_string();
        Ok(ReplyCause::CancelRequested)
    }

    pub fn complete_cancelled(&mut self, text: String) -> ReplyCause {
        let chars = text.chars().count();
        self.text = text;
        self.status = "cancelled".to_string();
        ReplyCause::Cancelled { chars }
    }

    pub fn is_open(&self) -> bool {
        self.status == "streaming" || self.status == "cancelling"
    }
}

service_engine::gated! {
    ReplyRow, AppPrincipal;
    "cancel" => fn cancel_gate(this, _principal) {
        if this.status == "streaming" {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_STREAMING)
        }
    }
}

fn row_to_reply(row: &sqlx::postgres::PgRow) -> ReplyRow {
    ReplyRow {
        id: row.get("id"),
        board_id: row.get("board_id"),
        org_id: row.get("org_id"),
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

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "reply", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, ReplyRow)>, EngineError>> {
        Box::pin(async move {
            Ok(load_replies_conn(conn, keys)
                .await?
                .into_iter()
                .map(|row| (row.id, row))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        reply: &'a ReplyRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO reply (id, board_id, org_id, text, status, blob_ref) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (id) DO UPDATE SET text = EXCLUDED.text, status = EXCLUDED.status, \
                   blob_ref = EXCLUDED.blob_ref",
            )
            .bind(reply.id)
            .bind(reply.board_id)
            .bind(reply.org_id)
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
                "INSERT INTO reply (id, board_id, org_id, text, status, blob_ref) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(reply.id)
            .bind(reply.board_id)
            .bind(reply.org_id)
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

pub async fn candidate_replies(pg: &PgPool) -> Result<Vec<(Uuid, ReplyRow)>, EngineError> {
    let rows = sqlx::query("SELECT id, board_id, org_id, text, status, blob_ref FROM reply")
        .fetch_all(pg)
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let reply = row_to_reply(row);
            (reply.id, reply)
        })
        .collect())
}

pub async fn load_replies_conn(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<ReplyRow>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, board_id, org_id, text, status, blob_ref FROM reply WHERE id = ANY($1)",
    )
    .bind(keys)
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(row_to_reply).collect())
}
