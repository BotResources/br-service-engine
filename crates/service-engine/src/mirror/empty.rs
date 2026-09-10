use crate::error::EngineError;

pub(super) fn empty_prefix(prefix: &'static str) -> EngineError {
    EngineError::Service(Box::new(EmptyPrefix { prefix }))
}

#[derive(Debug)]
struct EmptyPrefix {
    prefix: &'static str,
}

impl std::fmt::Display for EmptyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consumed prefix {} read empty; readiness stays down and the mirror is kept as it is",
            self.prefix
        )
    }
}

impl std::error::Error for EmptyPrefix {}
