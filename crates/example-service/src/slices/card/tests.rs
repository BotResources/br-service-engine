use uuid::Uuid;

use super::CARD_ADVANCE;
use super::aggregate::{CardEvent, CardState, Status};
use crate::kernel::AppPrincipal;

fn holder() -> AppPrincipal {
    AppPrincipal::new(
        Uuid::now_v7(),
        Uuid::now_v7(),
        vec![CARD_ADVANCE.to_string()],
        false,
    )
}

#[test]
fn a_new_card_starts_in_todo_and_records_a_created_fact() {
    let card = CardState::open(Uuid::now_v7(), Uuid::now_v7(), "Draft".to_string());
    assert_eq!(card.status, Status::Todo);
    assert!(matches!(card.pending(), [CardEvent::Created { .. }]));
}

#[test]
fn advancing_walks_todo_to_doing_to_done_and_appends_a_fact_each_step() {
    let mut card = CardState::open(Uuid::now_v7(), Uuid::now_v7(), "Draft".to_string());
    card.advance(&holder()).expect("todo advances");
    assert_eq!(card.status, Status::Doing);
    card.advance(&holder()).expect("doing advances");
    assert_eq!(card.status, Status::Done);
    assert!(matches!(
        card.pending().last(),
        Some(CardEvent::Advanced { to: Status::Done })
    ));
}

#[test]
fn advancing_a_done_card_is_refused() {
    let mut card = CardState::open(Uuid::now_v7(), Uuid::now_v7(), "Draft".to_string());
    card.advance(&holder()).unwrap();
    card.advance(&holder()).unwrap();
    let refused = card.advance(&holder()).expect_err("done is terminal");
    assert_eq!(refused.code(), "card_already_done");
}

#[test]
fn advancing_without_the_scope_is_refused() {
    let mut card = CardState::open(Uuid::now_v7(), Uuid::now_v7(), "Draft".to_string());
    let bystander = AppPrincipal::new(Uuid::now_v7(), Uuid::now_v7(), Vec::new(), false);
    let refused = card.advance(&bystander).expect_err("advance is gated");
    assert_eq!(refused.code(), "missing_advance_scope");
}

#[test]
fn a_deadline_forces_an_open_card_to_done() {
    let mut card = CardState::open(Uuid::now_v7(), Uuid::now_v7(), "Draft".to_string());
    card.pass_deadline();
    assert_eq!(card.status, Status::Done);
    assert!(matches!(
        card.pending().last(),
        Some(CardEvent::DeadlinePassed)
    ));
}
