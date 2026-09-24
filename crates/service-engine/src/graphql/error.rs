use async_graphql::{Context, Error, ErrorExtensions};

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

pub(crate) fn internal() -> Error {
    coded_error(INTERNAL_CODE, INTERNAL_MESSAGE)
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

pub trait OrInternal<T> {
    fn or_internal(self, context: &'static str) -> Result<T, Error>;
}

impl<T, E: std::error::Error + 'static> OrInternal<T> for Result<T, E> {
    fn or_internal(self, context: &'static str) -> Result<T, Error> {
        self.map_err(|error| internal_error(context, &error))
    }
}
