use async_graphql::{Context, Error, ErrorExtensions};

use crate::error::{AttachError, EngineError};
use crate::pipeline::MutationError;

pub const CODE_EXTENSION: &str = "code";
pub const FORBIDDEN_CODE: &str = "FORBIDDEN";
/// Every failure the client cannot fix: infrastructure, engine wiring, a
/// service fault without a reason. The message is [`INTERNAL_MESSAGE`]; the
/// cause, with its whole `source()` chain, is in the server log.
pub const INTERNAL_CODE: &str = "INTERNAL";
pub const INTERNAL_MESSAGE: &str = "internal error";
/// A page request that names no live session of the caller, or no window of it.
pub const NOT_FOUND_CODE: &str = "NOT_FOUND";
/// An attach under a session id that a live session already holds.
pub const CONFLICT_CODE: &str = "CONFLICT";
/// An attach whose principal no longer exists.
pub const UNAUTHENTICATED_CODE: &str = "UNAUTHENTICATED";

pub fn coded_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, extensions| {
        extensions.set(CODE_EXTENSION, code);
    })
}

pub fn forbidden() -> Error {
    coded_error(FORBIDDEN_CODE, "forbidden")
}

/// Logs `cause` with its whole `source()` chain under `context`, and answers
/// the client with `INTERNAL` and a fixed message that carries no detail.
pub fn internal_error(context: &'static str, cause: &(dyn std::error::Error + 'static)) -> Error {
    tracing::error!(
        context,
        cause = %crate::chain::describe(cause),
        "a graphql request failed on an internal fault; the client receives INTERNAL"
    );
    internal()
}

/// Logs `detail`, a fault with no error value behind it (a wiring mistake the
/// engine detects itself), and answers the client with `INTERNAL`.
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

/// A refusal keeps its reason code and detail. A failure without a reason
/// answers `INTERNAL`; the pipeline logged its cause where it was raised.
pub fn mutation_error(error: MutationError) -> Error {
    match error.code() {
        Some(code) => coded_error(code, format!("mutation refused: {}", error.detail)),
        None => internal(),
    }
}

pub(crate) fn attach_error(error: AttachError) -> Error {
    match &error {
        AttachError::DuplicateSession { .. } => coded_error(CONFLICT_CODE, error.to_string()),
        AttachError::PrincipalRevoked => coded_error(UNAUTHENTICATED_CODE, error.to_string()),
        _ => internal_error("attach a subscription", &error),
    }
}

pub(crate) fn page_error(error: EngineError) -> Error {
    match &error {
        EngineError::NoLiveSession { .. } | EngineError::NoSuchWindow { .. } => {
            coded_error(NOT_FOUND_CODE, error.to_string())
        }
        _ => internal_error("page a window", &error),
    }
}

/// The engine's own request data (state, principal). Its absence is a wiring
/// fault, never the client's.
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
    use crate::gate::Reason;
    use crate::session::SessionId;

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
            Some(Reason::new("NOT_OWNER")),
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
    fn a_page_refusal_the_client_can_fix_is_not_found_and_anything_else_internal() {
        let refused = page_error(EngineError::NoLiveSession {
            session: SessionId::new(),
        });
        assert_eq!(code(&refused), Some(Value::from(NOT_FOUND_CODE)));
        assert_internal(&page_error(EngineError::Db(sqlx::Error::PoolTimedOut)));
    }

    #[test]
    fn an_attach_refusal_the_client_can_fix_keeps_a_code_and_anything_else_is_internal() {
        let revoked = attach_error(AttachError::PrincipalRevoked);
        assert_eq!(code(&revoked), Some(Value::from(UNAUTHENTICATED_CODE)));
        let duplicate = attach_error(AttachError::DuplicateSession {
            session: SessionId::new(),
        });
        assert_eq!(code(&duplicate), Some(Value::from(CONFLICT_CODE)));
        assert_internal(&attach_error(AttachError::HeldImpacts(EngineError::Db(
            sqlx::Error::PoolTimedOut,
        ))));
    }
}
