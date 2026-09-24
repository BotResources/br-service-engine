use async_graphql::{Context, Error, ErrorExtensions};

use crate::error::AttachError;
use crate::pipeline::MutationError;

pub const CODE_EXTENSION: &str = "code";
pub const FORBIDDEN_CODE: &str = "FORBIDDEN";
pub const INTERNAL_CODE: &str = "INTERNAL";
pub const INTERNAL_MESSAGE: &str = "internal error";
pub const UNAUTHENTICATED_CODE: &str = "UNAUTHENTICATED";
pub const WINDOW_TOO_LARGE_CODE: &str = "WINDOW_TOO_LARGE";

pub fn coded_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, extensions| {
        extensions.set(CODE_EXTENSION, code);
    })
}

pub fn forbidden() -> Error {
    coded_error(FORBIDDEN_CODE, "forbidden")
}

pub fn internal_error(context: &'static str, cause: &(dyn std::error::Error + 'static)) -> Error {
    tracing::error!(
        context,
        cause = %crate::chain::describe(cause),
        "a graphql request failed on an internal fault; the client receives INTERNAL"
    );
    internal()
}

pub fn internal_fault(detail: impl std::fmt::Display) -> Error {
    tracing::error!(
        %detail,
        "a graphql request failed on an internal fault; the client receives INTERNAL"
    );
    internal()
}

fn internal() -> Error {
    coded_error(INTERNAL_CODE, INTERNAL_MESSAGE)
}

pub fn mutation_error(error: MutationError) -> Error {
    match (error.code(), error.refusal_detail()) {
        (Some(code), Some(detail)) => coded_error(code, format!("mutation refused: {detail}")),
        _ => internal(),
    }
}

pub(crate) fn attach_error(error: AttachError) -> Error {
    match &error {
        AttachError::PrincipalRevoked => coded_error(UNAUTHENTICATED_CODE, error.to_string()),
        AttachError::WindowTooLarge { capacity, .. } => coded_error(
            WINDOW_TOO_LARGE_CODE,
            format!(
                "the window holds more than {capacity} keys; narrow it with the window's arguments"
            ),
        ),
        _ => internal_error("attach a subscription", &error),
    }
}

pub(crate) fn engine_data<'c, T: std::any::Any + Send + Sync>(
    ctx: &Context<'c>,
) -> Result<&'c T, Error> {
    ctx.data::<T>().map_err(|error| {
        tracing::error!(
            detail = %error.message,
            "the graphql context lacks engine data; the client receives INTERNAL"
        );
        internal()
    })
}

pub(crate) trait OrInternal<T> {
    fn or_internal(self, context: &'static str) -> Result<T, Error>;
}

impl<T, E: std::error::Error + 'static> OrInternal<T> for Result<T, E> {
    fn or_internal(self, context: &'static str) -> Result<T, Error> {
        self.map_err(|error| internal_error(context, &error))
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::Value;

    use super::*;
    use crate::error::EngineError;
    use crate::gate::Reason;
    use crate::name::ProjectorName;

    fn code(error: &Error) -> Option<Value> {
        error
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get(CODE_EXTENSION).cloned())
    }

    fn assert_internal(error: &Error) {
        assert_eq!(code(error), Some(Value::from(INTERNAL_CODE)));
        assert_eq!(error.message, INTERNAL_MESSAGE);
    }

    #[test]
    fn a_mutation_failure_without_a_reason_is_internal_and_carries_no_detail() {
        assert_internal(&mutation_error(MutationError::internal(
            "error returned from database: relation \"secret_table\" does not exist",
        )));
    }

    #[test]
    fn a_refusal_keeps_its_reason_code_and_detail() {
        let error = mutation_error(MutationError::refused(
            Reason::new("NOT_OWNER"),
            "the caller does not own the board",
        ));
        assert_eq!(code(&error), Some(Value::from("NOT_OWNER")));
        assert_eq!(
            error.message,
            "mutation refused: the caller does not own the board"
        );
    }

    #[test]
    fn an_engine_fault_reaches_the_client_as_internal_without_its_cause() {
        let fault = EngineError::Db(sqlx::Error::Protocol("password for role x leaked".into()));
        let answered = Err::<(), _>(fault).or_internal("render a query");
        assert_internal(&answered.expect_err("the fault stays an error"));
    }

    #[test]
    fn an_attach_refusal_the_client_can_fix_keeps_a_code_and_anything_else_is_internal() {
        let revoked = attach_error(AttachError::PrincipalRevoked);
        assert_eq!(code(&revoked), Some(Value::from(UNAUTHENTICATED_CODE)));
        let oversized = attach_error(AttachError::WindowTooLarge {
            projector: ProjectorName::from_static("hidden_projector"),
            size: 12_345,
            capacity: 10,
        });
        assert_eq!(code(&oversized), Some(Value::from(WINDOW_TOO_LARGE_CODE)));
        assert!(
            oversized.message.contains("10"),
            "the client learns the bound it must narrow under: {}",
            oversized.message
        );
        assert!(
            !oversized.message.contains("12345") && !oversized.message.contains("hidden_projector"),
            "the refusal names neither the projector nor the population size, which may count \
             rows the caller cannot see: {}",
            oversized.message
        );
        assert_internal(&attach_error(AttachError::HeldImpacts(EngineError::Db(
            sqlx::Error::PoolTimedOut,
        ))));
        assert_internal(&attach_error(AttachError::PrincipalRefreshFailed));
    }
}
