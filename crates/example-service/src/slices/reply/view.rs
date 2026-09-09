use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, View};
use sqlx::PgConnection;
use uuid::Uuid;

use super::aggregate::{Reply, ReplyRow, all_reply_ids, load_replies_conn};
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ReplyView {
    pub id: Uuid,
    pub board_id: Uuid,
    pub text: String,
    pub status: String,
    pub has_attachment: bool,
    pub affordances: Affordances,
}

#[derive(Default)]
pub struct RepliesView;

impl RepliesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("replies");
}

impl View for RepliesView {
    type Principal = AppPrincipal;
    type Noun = Reply;
    type Row = ReplyRow;
    type Query = ();
    type Out = ReplyView;

    const NAME: ProjectorName = Self::NAME;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> service_engine::view::RowsFuture<'a, Uuid, ReplyRow> {
        Box::pin(async move {
            Ok(load_replies_conn(conn, keys)
                .await?
                .into_iter()
                .map(|row| (row.id, row))
                .collect())
        })
    }

    fn populate<'a>(
        cx: &'a Populate<'a, AppPrincipal>,
        _query: &'a (),
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Keys(
                all_reply_ids(cx.pool()).await?.into_iter().collect(),
            ))
        })
    }

    fn project(row: &ReplyRow, principal: &AppPrincipal) -> ReplyView {
        ReplyView {
            id: row.id,
            board_id: row.board_id,
            text: row.text.clone(),
            status: row.status.clone(),
            has_attachment: row.blob_ref.is_some(),
            affordances: row.affordances(principal),
        }
    }

    fn emission() -> Emission {
        Emission::PerImpact
    }
}
