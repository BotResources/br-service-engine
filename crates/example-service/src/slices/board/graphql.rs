use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::graphql::SliceFragment;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use super::mutations::{ArchiveBoard, CreateBoard, MintBoardInvite, SetBoardMembership};
use super::view::{BoardFilter, BoardView, BoardsView, OrgBoardsRls};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = BoardViewUnion;
    delta = BoardDelta { reset = BoardReset, upsert = BoardUpsert, remove = BoardRemove };
    Board => service_engine::view::ViewProjector<BoardsView> => BoardView,
}

pub const FRAGMENT: SliceFragment = SliceFragment {
    slice: "board",
    root_fields: &[
        "board",
        "boards",
        "orgBoards",
        "boardDeltas",
        "createBoard",
        "archiveBoard",
        "mintBoardInvite",
        "setBoardMembership",
    ],
    types: &["BoardView"],
};

#[derive(Default)]
pub struct BoardQuery;

#[Object]
impl BoardQuery {
    async fn board(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<BoardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view::<BoardsView>(&id)
            .await
    }

    async fn boards(&self, ctx: &Context<'_>) -> Result<Vec<BoardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<BoardsView>(&BoardFilter::default())
            .await
    }

    async fn org_boards(&self, ctx: &Context<'_>) -> Result<Vec<BoardView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<OrgBoardsRls>(&())
            .await
    }
}

#[derive(Default)]
pub struct BoardMutation;

#[Object]
impl BoardMutation {
    async fn create_board(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        name: String,
        is_public: bool,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, CreateBoard>(
            ctx,
            CreateBoard {
                id,
                name,
                is_public,
            },
        )
        .await
    }

    async fn archive_board(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, ArchiveBoard>(ctx, ArchiveBoard { id }).await
    }

    async fn mint_board_invite(&self, ctx: &Context<'_>, id: Uuid) -> Result<String> {
        let token =
            service_engine::execute::<AppPrincipal, MintBoardInvite>(ctx, MintBoardInvite { id })
                .await?;
        Ok(token.into_inner())
    }

    async fn set_board_membership(
        &self,
        ctx: &Context<'_>,
        board_id: Uuid,
        user_id: Uuid,
        member: bool,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, SetBoardMembership>(
            ctx,
            SetBoardMembership {
                board_id,
                user_id,
                member,
            },
        )
        .await
    }
}

#[derive(Default)]
pub struct BoardSubscription;

#[Subscription]
impl BoardSubscription {
    async fn board_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<BoardDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(
                BoardsView::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| BoardDelta::from_delta(&delta)))
    }
}
