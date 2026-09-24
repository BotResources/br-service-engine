use async_graphql::{EmptyMutation, EmptySubscription, Error, Object, Request, Result, Schema};
use serde_json::{Value, json};

use crate::error::{AttachError, EngineError};
use crate::gate::{Gate, Reason};
use crate::graphql::error::{
    CODE_EXTENSION, INTERNAL_CODE, INTERNAL_MESSAGE, OrInternal, UNAUTHENTICATED_CODE,
    WINDOW_TOO_LARGE_CODE,
};
use crate::name::ProjectorName;
use crate::page::{WINDOW_SIZE_INVALID_CODE, WindowSize};
use crate::pipeline::MutationError;

const NOT_OWNER: Reason = Reason::new("NOT_OWNER");
const DATABASE_TEXT: &str =
    "error returned from database: relation \"secret_table\" does not exist";

fn refused_mutation() -> Result<(), MutationError> {
    Err(MutationError::refused(
        NOT_OWNER,
        "the caller does not own the board",
    ))
}

fn failed_mutation() -> Result<(), MutationError> {
    Err(MutationError::internal(DATABASE_TEXT))
}

fn oversized_window() -> Result<(), AttachError> {
    Err(AttachError::WindowTooLarge {
        projector: ProjectorName::from_static("hidden_projector"),
        size: 12_345,
        capacity: 10,
    })
}

fn engine_fault() -> Result<bool, EngineError> {
    Err(EngineError::Db(sqlx::Error::Protocol(
        "password for role x leaked".into(),
    )))
}

struct Resolvers;

#[Object]
impl Resolvers {
    async fn blocked_gate(&self) -> Result<bool> {
        Gate::blocked(NOT_OWNER).require()?;
        Ok(true)
    }

    async fn refused_mutation(&self) -> Result<bool> {
        refused_mutation()?;
        Ok(true)
    }

    async fn failed_mutation(&self) -> Result<bool> {
        failed_mutation()?;
        Ok(true)
    }

    async fn oversized_window(&self) -> Result<bool> {
        oversized_window()?;
        Ok(true)
    }

    async fn zero_window_size(&self) -> Result<bool> {
        WindowSize::new(0)?;
        Ok(true)
    }

    async fn engine_fault(&self) -> Result<bool> {
        engine_fault().or_internal("read a board")
    }
}

async fn answer(field: &str) -> (Value, Value) {
    let schema = Schema::new(Resolvers, EmptyMutation, EmptySubscription);
    let response = schema.execute(Request::new(format!("{{ {field} }}"))).await;
    let body = serde_json::to_value(&response).expect("a graphql response serializes");
    assert!(
        body["data"].is_null(),
        "a refused or failed field carries no datum: {body}"
    );
    let error = body["errors"][0].clone();
    (
        error["extensions"]["code"].clone(),
        error["message"].clone(),
    )
}

#[tokio::test]
async fn a_blocked_gate_forwarded_with_question_mark_answers_its_reason_code() {
    let (code, message) = answer("blockedGate").await;

    assert_eq!(code, json!("NOT_OWNER"));
    assert_eq!(message, json!("NOT_OWNER"));
}

#[tokio::test]
async fn a_mutation_refusal_forwarded_with_question_mark_keeps_its_code_and_detail() {
    let (code, message) = answer("refusedMutation").await;

    assert_eq!(code, json!("NOT_OWNER"));
    assert_eq!(
        message,
        json!("mutation refused: the caller does not own the board")
    );
}

#[tokio::test]
async fn a_mutation_failure_forwarded_with_question_mark_is_internal_without_its_cause() {
    let (code, message) = answer("failedMutation").await;

    assert_eq!(code, json!(INTERNAL_CODE));
    assert_eq!(message, json!(INTERNAL_MESSAGE));
}

#[tokio::test]
async fn an_oversized_window_forwarded_with_question_mark_names_the_capacity_only() {
    let (code, message) = answer("oversizedWindow").await;

    assert_eq!(code, json!(WINDOW_TOO_LARGE_CODE));
    let message = message.as_str().expect("the message is text");
    assert!(
        message.contains("10"),
        "the client learns the bound it must narrow under: {message}"
    );
    assert!(
        !message.contains("12345") && !message.contains("hidden_projector"),
        "the refusal names neither the projector nor the population size, which may count rows \
         the caller cannot see: {message}"
    );
}

#[tokio::test]
async fn a_window_size_out_of_range_forwarded_with_question_mark_is_window_size_invalid() {
    let (code, _message) = answer("zeroWindowSize").await;

    assert_eq!(code, json!(WINDOW_SIZE_INVALID_CODE));
}

#[tokio::test]
async fn a_fault_named_with_or_internal_is_internal_without_its_cause() {
    let (code, message) = answer("engineFault").await;

    assert_eq!(code, json!(INTERNAL_CODE));
    assert_eq!(message, json!(INTERNAL_MESSAGE));
}

#[test]
fn an_attach_refusal_the_client_can_fix_keeps_a_code_and_any_other_is_internal() {
    let code = |error: Error| {
        error
            .extensions
            .and_then(|extensions| extensions.get(CODE_EXTENSION).cloned())
    };

    assert_eq!(
        code(Error::from(AttachError::PrincipalRevoked)),
        Some(async_graphql::Value::from(UNAUTHENTICATED_CODE))
    );
    assert_eq!(
        code(Error::from(AttachError::HeldImpacts(EngineError::Db(
            sqlx::Error::PoolTimedOut,
        )))),
        Some(async_graphql::Value::from(INTERNAL_CODE))
    );
    assert_eq!(
        code(Error::from(AttachError::PrincipalRefreshFailed)),
        Some(async_graphql::Value::from(INTERNAL_CODE))
    );
}
