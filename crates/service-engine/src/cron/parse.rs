use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};

use crate::error::CronError;

fn five_field_parser() -> CronParser {
    CronParser::builder()
        .seconds(Seconds::Disallowed)
        .year(Year::Disallowed)
        .build()
}

pub(super) fn parse(expr: &str) -> Result<Cron, CronError> {
    five_field_parser()
        .parse(expr)
        .map_err(|reason| CronError::Expr {
            expr: expr.to_string(),
            reason: reason.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Timestamp;

    #[test]
    fn a_five_field_expression_parses() {
        for expr in [
            "* * * * *",
            "*/15 * * * *",
            "0 3 L * *",
            "0 0 13 * FRI",
            "0 0 29 2 *",
            "30 4 1,15 * SUN",
        ] {
            assert!(parse(expr).is_ok(), "{expr}");
        }
    }

    #[test]
    fn a_field_count_other_than_five_is_refused_with_a_typed_error() {
        for wrong in ["", "* * * *", "0 * * * * *", "0 0 * * * * *"] {
            let Err(CronError::Expr { expr, reason }) = parse(wrong) else {
                panic!("{wrong} was accepted where five fields are the contract");
            };
            assert_eq!(expr, wrong);
            assert!(!reason.is_empty());
        }
    }

    #[test]
    fn a_field_outside_its_range_or_alphabet_is_refused_with_a_typed_error() {
        for wrong in [
            "60 * * * *",
            "* 24 * * *",
            "* * 32 * *",
            "* * * 13 *",
            "* * * * 8",
            "* * * * BLAH",
            "*/0 * * * *",
            "5-1 * * * *",
        ] {
            assert!(
                matches!(parse(wrong), Err(CronError::Expr { .. })),
                "{wrong} was accepted"
            );
        }
    }

    #[test]
    fn a_nickname_expands_to_the_five_field_expression_it_names() {
        for (nickname, spelled_out) in [
            ("@yearly", "0 0 1 1 *"),
            ("@annually", "0 0 1 1 *"),
            ("@monthly", "0 0 1 * *"),
            ("@weekly", "0 0 * * 0"),
            ("@daily", "0 0 * * *"),
            ("@hourly", "0 * * * *"),
        ] {
            let after: Timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
            let by_nickname = parse(nickname)
                .unwrap()
                .find_next_occurrence(&after.as_datetime(), false)
                .unwrap();
            let by_hand = parse(spelled_out)
                .unwrap()
                .find_next_occurrence(&after.as_datetime(), false)
                .unwrap();
            assert_eq!(by_nickname, by_hand, "{nickname}");
        }
    }

    #[test]
    fn the_seconds_field_is_not_silently_accepted_as_a_sixth_column() {
        let refused = parse("0 30 4 * * *");
        assert!(matches!(refused, Err(CronError::Expr { .. })));
    }
}
