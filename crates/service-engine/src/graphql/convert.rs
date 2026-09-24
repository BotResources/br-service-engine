use async_graphql::Error;

use crate::error::AttachError;
use crate::gate::Reason;
use crate::graphql::error::{
    UNAUTHENTICATED_CODE, WINDOW_TOO_LARGE_CODE, coded_error, internal, internal_error,
};
use crate::page::{WINDOW_SIZE_INVALID_CODE, WindowSizeOutOfRange};
use crate::pipeline::MutationError;

impl From<Reason> for Error {
    fn from(reason: Reason) -> Self {
        coded_error(reason.code(), reason.code())
    }
}

impl From<MutationError> for Error {
    fn from(error: MutationError) -> Self {
        match (error.code(), error.refusal_detail()) {
            (Some(code), Some(detail)) => coded_error(code, format!("mutation refused: {detail}")),
            _ => internal(),
        }
    }
}

impl From<AttachError> for Error {
    fn from(error: AttachError) -> Self {
        match &error {
            AttachError::PrincipalRevoked => coded_error(UNAUTHENTICATED_CODE, error.to_string()),
            AttachError::WindowTooLarge { capacity, .. } => coded_error(
                WINDOW_TOO_LARGE_CODE,
                format!(
                    "the window holds more than {capacity} keys; narrow it with the window's \
                     arguments"
                ),
            ),
            _ => internal_error("attach a window", &error),
        }
    }
}

pub(crate) fn one_key_refusal(error: AttachError) -> Error {
    let AttachError::WindowTooLarge {
        projector,
        keys_read,
        capacity,
    } = &error
    else {
        return Error::from(error);
    };
    tracing::warn!(
        %projector,
        keys_read,
        capacity,
        "a raw one-key fetch was refused: the projector's default window holds more than \
         window_capacity keys, so it cannot decide membership; read the key through fetch_view"
    );
    coded_error(
        WINDOW_TOO_LARGE_CODE,
        format!(
            "this key is looked up in a window that holds more than {capacity} keys; no argument \
             of this field narrows it, the service must read the key by its row"
        ),
    )
}

impl From<WindowSizeOutOfRange> for Error {
    fn from(error: WindowSizeOutOfRange) -> Self {
        coded_error(WINDOW_SIZE_INVALID_CODE, error.to_string())
    }
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
