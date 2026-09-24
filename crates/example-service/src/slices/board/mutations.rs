use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use service_engine::principal::PrincipalId;
use uuid::Uuid;

use super::aggregate::{Board, BoardCause, BoardRow, BoardState, MEMBERSHIP_DEPS};
use super::store;
use crate::kernel::{AppFault, AppPrincipal};

#[derive(Debug, Deserialize)]
pub struct CreateBoard {
    pub id: Uuid,
    pub name: String,
    pub is_public: bool,
}

impl MutationInput for CreateBoard {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "create_board";
}

pub fn create_board<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: CreateBoard,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let org = cx.principal().org();
        let user = cx.principal().user();
        let board = BoardRow {
            id: input.id,
            org_id: org,
            name: input.name,
            is_public: input.is_public,
            state: BoardState::Active,
        };
        cx.create(&board).await?;
        store::add_member(cx.connection(), board.id, user).await?;
        cx.impact_caused::<Board, _>(&board.id, BoardCause::Created)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct ArchiveBoard {
    pub id: Uuid,
}

impl MutationInput for ArchiveBoard {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "archive_board";
}

pub fn archive_board<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: ArchiveBoard,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let mut board = cx
            .load::<BoardRow>(&input.id)
            .await?
            .ok_or(AppFault::NotFound)?;
        board.archive(cx.principal())?;
        cx.save(&board).await?;
        cx.impact_caused::<Board, _>(&board.id, BoardCause::Archived)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct MintBoardInvite {
    pub id: Uuid,
}

impl MutationInput for MintBoardInvite {
    type Output = OneShot<String>;
    type Error = AppFault;
    const NAME: &'static str = "mint_board_invite";
}

pub fn mint_board_invite<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: MintBoardInvite,
) -> BoxFuture<'m, Result<OneShot<String>, AppFault>> {
    Box::pin(async move {
        let board = cx
            .load::<BoardRow>(&input.id)
            .await?
            .ok_or(AppFault::NotFound)?;
        let token = format!("invite-{}-{}", board.id.simple(), Uuid::now_v7().simple());
        cx.impact_caused::<Board, _>(&board.id, BoardCause::InviteMinted)?;
        Ok(OneShot(token))
    })
}

#[derive(Debug, Deserialize)]
pub struct SetBoardMembership {
    pub board_id: Uuid,
    pub user_id: Uuid,
    pub member: bool,
}

impl MutationInput for SetBoardMembership {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "set_board_membership";
}

pub fn set_board_membership<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: SetBoardMembership,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let board = cx
            .load::<BoardRow>(&input.board_id)
            .await?
            .ok_or(AppFault::NotFound)?;
        board.manage_members_gate(cx.principal()).require()?;
        if input.member {
            store::add_member(cx.connection(), input.board_id, input.user_id).await?;
        } else {
            store::remove_member(cx.connection(), input.board_id, input.user_id).await?;
        }
        cx.impact_principal_facts(PrincipalId::from(input.user_id), MEMBERSHIP_DEPS);
        Ok(())
    })
}
