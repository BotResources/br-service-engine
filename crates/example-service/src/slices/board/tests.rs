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
    AppPrincipal::new(
        Uuid::now_v7(),
        org,
        vec![BOARD_ARCHIVE.to_string()],
        false,
    )
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
    assert_eq!(refused.code(), "missing_archive_scope");
    assert_eq!(board.state, BoardState::Active);
}

#[test]
fn archiving_an_already_archived_board_is_refused_as_not_active() {
    let org = Uuid::now_v7();
    let mut board = board(BoardState::Archived);
    let refused = board
        .archive(&holder(org))
        .expect_err("a second archive is refused");
    assert_eq!(refused.code(), "board_not_active");
}

#[test]
fn the_gate_and_the_command_agree_on_the_same_reason() {
    let org = Uuid::now_v7();
    let board = board(BoardState::Archived);
    let gate = board.archive_gate(&holder(org));
    assert!(!gate.is_allowed());
    assert_eq!(gate.reason().map(|r| r.code()), Some("board_not_active"));
}
