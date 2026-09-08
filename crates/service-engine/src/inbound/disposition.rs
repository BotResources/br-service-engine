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
    fn a_disposition_is_a_plain_copy_choice() {
        assert_eq!(Disposition::Retry, Disposition::Retry);
        assert_ne!(Disposition::Retry, Disposition::Terminal);
    }
}
