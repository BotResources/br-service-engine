use crate::error::EngineError;
use crate::inbound::DispatchError;
use crate::inbound::sqlx_is_terminal;

pub(crate) fn classify(error: &sqlx::Error) -> DispatchError {
    if sqlx_is_terminal(error) {
        DispatchError::terminal(error.to_string())
    } else {
        DispatchError::retry(error.to_string())
    }
}

pub(crate) fn classify_engine(error: &EngineError) -> DispatchError {
    match error {
        EngineError::Db(db) => classify(db),
        EngineError::Encode { .. } | EngineError::Decode { .. } | EngineError::Config(_) => {
            DispatchError::terminal(error.to_string())
        }
        _ => DispatchError::retry(error.to_string()),
    }
}

pub(crate) fn classify_engine_logged(context: &'static str, error: &EngineError) -> DispatchError {
    crate::inbound::log_reaction_db_fault(context, error);
    classify_engine(error)
}
