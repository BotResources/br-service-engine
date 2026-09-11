use std::fmt;

use croner::Cron;

use crate::error::CronError;
use crate::time::Timestamp;

use super::next::{self, NextFire};
use super::parse;

#[derive(Debug, Clone)]
pub struct CronExpr {
    source: String,
    cron: Box<Cron>,
}

impl CronExpr {
    pub fn new(expr: impl Into<String>) -> Result<Self, CronError> {
        let source = expr.into();
        let cron = Box::new(parse::parse(&source)?);
        Ok(Self { source, cron })
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub fn next_fire(&self, after: Timestamp) -> Result<NextFire, CronError> {
        next::cron_next_fire(&self.cron, &self.source, after)
    }

    pub fn previous_fire(&self, before: Timestamp) -> Result<NextFire, CronError> {
        next::cron_previous_fire(&self.cron, &self.source, before)
    }
}

impl PartialEq for CronExpr {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for CronExpr {}

impl fmt::Display for CronExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

impl AsRef<str> for CronExpr {
    fn as_ref(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expression_keeps_the_text_the_service_wrote() {
        let expr = CronExpr::new("0 3 * * mon").unwrap();
        assert_eq!(expr.as_str(), "0 3 * * mon");
        assert_eq!(expr.to_string(), "0 3 * * mon");
    }

    #[test]
    fn a_malformed_expression_is_refused_at_construction() {
        assert!(matches!(
            CronExpr::new("0 3 * *"),
            Err(CronError::Expr { .. })
        ));
    }

    #[test]
    fn two_expressions_are_equal_when_their_text_is() {
        assert_eq!(
            CronExpr::new("0 0 * * *").unwrap(),
            CronExpr::new("0 0 * * *").unwrap()
        );
        assert_ne!(
            CronExpr::new("0 0 * * *").unwrap(),
            CronExpr::new("0 1 * * *").unwrap()
        );
    }
}
