use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::aggregate::{Reply, ReplyRow, ReplyStore, all_reply_ids};
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

impl Projector for RepliesView {
    type Principal = AppPrincipal;
    type Noun = Reply;
    type Store = ReplyStore;
    type Query = ();
    type Out = ReplyView;
    type Visibility = Unrestricted<ReplyRow, AppPrincipal>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            all_reply_ids(cx.pool()).await?.into_iter().collect(),
        ))
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
