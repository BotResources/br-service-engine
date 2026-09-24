use uuid::Uuid;

use super::BOARD_ARCHIVE;
use super::aggregate::{BoardRow, BoardState};
use crate::kernel::AppPrincipal;

fn board(state: BoardState) -> BoardRow {
    BoardRow {
        id: Uuid::now_v7(),
        org_id: Uuid::now_v7(),
        name: "Board".to_string(),
        is_public: false,
        state,
    }
}

fn holder(org: Uuid) -> AppPrincipal {
    AppPrincipal::new(Uuid::now_v7(), org, vec![BOARD_ARCHIVE.to_string()], false)
}

fn bystander(org: Uuid) -> AppPrincipal {
    AppPrincipal::new(Uuid::now_v7(), org, Vec::new(), false)
}

#[test]
fn a_holder_of_the_scope_can_archive_an_active_board() {
    let org = Uuid::now_v7();
    let mut board = board(BoardState::Active);
    assert!(board.archive(&holder(org)).is_ok());
    assert_eq!(board.state, BoardState::Archived);
}

#[test]
fn archiving_without_the_scope_is_refused_with_the_missing_scope_reason() {
    let org = Uuid::now_v7();
    let mut board = board(BoardState::Active);
    let refused = board
        .archive(&bystander(org))
        .expect_err("archive is gated");
    assert_eq!(refused.code(), "MISSING_ARCHIVE_SCOPE");
    assert_eq!(board.state, BoardState::Active);
}

#[test]
fn archiving_an_already_archived_board_is_refused_as_not_active() {
    let org = Uuid::now_v7();
    let mut board = board(BoardState::Archived);
    let refused = board
        .archive(&holder(org))
        .expect_err("a second archive is refused");
    assert_eq!(refused.code(), "BOARD_NOT_ACTIVE");
}

#[test]
fn the_gate_and_the_command_agree_on_the_same_reason() {
    let org = Uuid::now_v7();
    let board = board(BoardState::Archived);
    let gate = board.archive_gate(&holder(org));
    assert!(!gate.is_allowed());
    assert_eq!(gate.reason().map(|r| r.code()), Some("BOARD_NOT_ACTIVE"));
}

#[test]
fn the_board_declaration_admits_the_org_public_and_member_cohorts_only() {
    use std::collections::BTreeSet;

    use service_engine::visibility::check_window_matches_visibility;

    use super::BoardMemberships;
    use super::aggregate::Board;

    let org = Uuid::now_v7();
    let other_org = Uuid::now_v7();

    let named = |org_id: Uuid, name: &str, is_public: bool| BoardRow {
        id: Uuid::now_v7(),
        org_id,
        name: name.to_string(),
        is_public,
        state: BoardState::Active,
    };
    let mine = named(org, "mine", false);
    let public = named(other_org, "public", true);
    let member_board = named(other_org, "member", false);
    let hidden = named(other_org, "hidden", false);

    let principal = AppPrincipal::new(Uuid::now_v7(), org, Vec::new(), false)
        .with_fact(BoardMemberships(vec![member_board.id]));

    let candidates: Vec<(Uuid, BoardRow)> = vec![
        (mine.id, mine.clone()),
        (public.id, public.clone()),
        (member_board.id, member_board.clone()),
        (hidden.id, hidden.clone()),
    ];

    check_window_matches_visibility::<Board, _, _>(
        &BTreeSet::from([mine.id, public.id, member_board.id]),
        candidates,
        &principal,
    )
    .expect("org, public and member cohorts are visible; a private board of another org is not");
}
