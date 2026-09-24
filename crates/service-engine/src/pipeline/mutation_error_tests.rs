use crate::error::describe;
use crate::gate::Reason;
use crate::pipeline::MutationError;
use crate::test_log::logged;

const CHAIN: &str =
    "the store failed: error returned from database: permission denied for table widgets";

#[test]
fn a_reasonless_failure_keeps_its_cause_chain_for_describe() {
    let failure = MutationError::internal(CHAIN);

    let described = describe(&failure);

    assert_eq!(described, format!("mutation failed: {CHAIN}"));
    assert_eq!(failure.refusal_detail(), None);
}

#[test]
fn a_refusal_writes_its_code_and_detail_and_has_no_hidden_cause() {
    let refusal = MutationError::refused(Reason::new("BOARD_CLOSED"), "the board is closed");

    let rendered = refusal.to_string();

    assert_eq!(
        rendered,
        "mutation refused (BOARD_CLOSED): the board is closed"
    );
    assert_eq!(describe(&refusal), rendered);
}

#[test]
fn a_failure_an_adopter_builds_is_logged_once_with_its_cause() {
    let (failure, lines) = logged(|| MutationError::internal(CHAIN));

    assert_eq!(
        lines.len(),
        1,
        "the public constructor logs the failure exactly once: {lines:?}"
    );
    assert!(
        lines[0].contains("ERROR") && lines[0].contains(CHAIN),
        "the log line is an error that keeps the cause the client never sees: {lines:?}"
    );
    assert_eq!(failure.refusal_detail(), None);
}

#[test]
fn a_refusal_is_not_logged_as_a_failure() {
    let (_refusal, lines) =
        logged(|| MutationError::refused(Reason::new("BOARD_CLOSED"), "closed"));

    assert!(
        lines.is_empty(),
        "a refusal is an answer, not a fault: {lines:?}"
    );
}
