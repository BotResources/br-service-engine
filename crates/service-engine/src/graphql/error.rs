use async_graphql::{Error, ErrorExtensions};

use crate::pipeline::MutationError;

pub const CODE_EXTENSION: &str = "code";
pub const FORBIDDEN_CODE: &str = "FORBIDDEN";

pub fn coded_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, extensions| {
        extensions.set(CODE_EXTENSION, code);
    })
}

pub fn forbidden() -> Error {
    coded_error(FORBIDDEN_CODE, "forbidden")
}

pub fn mutation_error(error: MutationError) -> Error {
    match error.code() {
        Some(code) => coded_error(code, format!("mutation refused: {}", error.detail)),
        None => Error::new(format!("mutation failed: {}", error.detail)),
    }
}
