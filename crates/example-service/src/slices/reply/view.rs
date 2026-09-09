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

use super::aggregate::{Reply, ReplyRow};
use super::aggregate::{all_reply_ids, load_replies, load_replies_conn};
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ReplyView {
    pub id: Uuid,
    pub board_id: Uuid,
    pub text: String,
    pub status: String,
    pub has_attachment: bool,
}

pub struct ReplyFacts {
    rows: BTreeMap<Uuid, ReplyRow>,
}

#[derive(Default)]
pub struct RepliesView;

impl RepliesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("replies");
}

impl Projector for RepliesView {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = ReplyFacts;
    type View = ReplyView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Reply::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        _principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Keys(
                all_reply_ids(pg).await?.into_iter().collect(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<ReplyFacts, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => load_replies(pg, &keys).await?,
                LoadScope::PerPrincipal { conn, .. } => load_replies_conn(conn, &keys).await?,
            };
            Ok(ReplyFacts {
                rows: rows.into_iter().map(|row| (row.id, row)).collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &ReplyFacts,
        key: &Uuid,
        _principal: &AppPrincipal,
    ) -> Option<ReplyView> {
        facts.rows.get(key).map(|row| ReplyView {
            id: row.id,
            board_id: row.board_id,
            text: row.text.clone(),
            status: row.status.clone(),
            has_attachment: row.blob_ref.is_some(),
        })
    }
}
