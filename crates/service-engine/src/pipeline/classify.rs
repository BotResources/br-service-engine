use crate::error::EngineError;
use crate::inbound::DispatchError;
use crate::inbound::sqlx_is_terminal;

pub(crate) fn classify(error: &sqlx::Error) -> DispatchError {
    let cause = crate::chain::describe(error);
    if sqlx_is_terminal(error) {
        DispatchError::terminal(cause)
    } else {
        DispatchError::retry(cause)
    }
}

pub(crate) fn classify_engine(error: &EngineError) -> DispatchError {
    match error {
        EngineError::Db(db) => classify(db),
        EngineError::Encode { .. } | EngineError::Decode { .. } | EngineError::Config(_) => {
            DispatchError::terminal(crate::chain::describe(error))
        }
        _ => DispatchError::retry(crate::chain::describe(error)),
    }
}

pub(crate) fn classify_engine_logged(context: &'static str, error: &EngineError) -> DispatchError {
    crate::inbound::log_reaction_db_fault(context, error);
    classify_engine(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inbound::Disposition;

    #[test]
    fn a_config_fault_on_the_emit_path_is_terminal_not_retryable() {
        let error = EngineError::Config("a producer sequence with no service".into());
        assert_eq!(classify_engine(&error).disposition, Disposition::Terminal);
    }

    #[test]
    fn a_non_terminal_engine_fault_stays_retryable() {
        let error = EngineError::Blob("a transient object-storage fault".into());
        assert_eq!(classify_engine(&error).disposition, Disposition::Retry);
    }
}
