use async_graphql::{Error, ErrorExtensions};

use crate::pipeline::MutationError;

pub const CODE_EXTENSION: &str = "code";

pub fn mutation_error(error: MutationError) -> Error {
    let code = error.code();
    let message = match error.reason {
        Some(_) => format!("mutation refused: {}", error.detail),
        None => format!("mutation failed: {}", error.detail),
    };
    Error::new(message).extend_with(|_, extensions| {
        if let Some(code) = code {
            extensions.set(CODE_EXTENSION, code);
        }
    })
}
