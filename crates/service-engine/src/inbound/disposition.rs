#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Retry,
    Park,
    Terminal,
}

pub trait ReactionError {
    fn disposition(&self) -> Disposition;
}

pub fn sqlx_is_terminal(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db) = error else {
        return false;
    };
    match db.code() {
        Some(code) => {
            let class = code.as_bytes();
            class.len() >= 2 && (&class[..2] == b"23" || &class[..2] == b"22")
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_database_error_is_not_classified_terminal() {
        assert!(!sqlx_is_terminal(&sqlx::Error::RowNotFound));
        assert!(!sqlx_is_terminal(&sqlx::Error::PoolClosed));
        assert!(!sqlx_is_terminal(&sqlx::Error::Protocol(
            "seq overflow".into()
        )));
    }
}
