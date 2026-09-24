use crate::error::describe;
use crate::gate::Reason;
use crate::pipeline::MutationError;

const CHAIN: &str =
    "the store failed: error returned from database: permission denied for table widgets";

fn resolver_forwarding_with_question_mark(
    outcome: Result<(), MutationError>,
) -> async_graphql::Result<()> {
    Ok(outcome?)
}

#[test]
fn a_reasonless_failure_forwarded_with_question_mark_reaches_the_client_without_its_cause() {
    let failure = MutationError::internal(CHAIN);

    let answered = resolver_forwarding_with_question_mark(Err(failure))
        .expect_err("the executor failed, so the resolver fails");

    assert_eq!(answered.message, "mutation failed");
    assert!(answered.extensions.is_none());
}

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
