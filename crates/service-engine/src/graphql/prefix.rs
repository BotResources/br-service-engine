use crate::error::EngineError;

const MAX_PREFIX_LEN: usize = 40;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootPrefix {
    snake: &'static str,
    lower_camel: String,
}

impl RootPrefix {
    pub fn from_snake(snake: &'static str) -> Result<Self, EngineError> {
        validate_snake(snake)?;
        Ok(Self {
            snake,
            lower_camel: to_lower_camel(snake),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.lower_camel
    }

    pub fn snake(&self) -> &'static str {
        self.snake
    }

    pub fn owns(&self, root_field: &str) -> bool {
        match root_field.strip_prefix(self.lower_camel.as_str()) {
            Some(tail) => tail
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_uppercase()),
            None => false,
        }
    }
}

fn validate_snake(snake: &'static str) -> Result<(), EngineError> {
    let invalid = |reason: &'static str| EngineError::RootPrefixInvalid {
        value: snake.to_string(),
        reason,
    };
    if snake.is_empty() {
        return Err(invalid("must not be empty"));
    }
    if snake.len() > MAX_PREFIX_LEN {
        return Err(invalid("must be at most 40 characters"));
    }
    let bytes = snake.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return Err(invalid("must start with a lowercase ASCII letter"));
    }
    if bytes[bytes.len() - 1] == b'_' {
        return Err(invalid("must not end with an underscore"));
    }
    let mut previous_underscore = false;
    for &b in bytes {
        if b == b'_' {
            if previous_underscore {
                return Err(invalid("must not contain a double underscore"));
            }
            previous_underscore = true;
        } else if b.is_ascii_lowercase() || b.is_ascii_digit() {
            previous_underscore = false;
        } else {
            return Err(invalid(
                "must contain only lowercase ASCII letters, digits and underscores",
            ));
        }
    }
    Ok(())
}

fn to_lower_camel(snake: &str) -> String {
    let mut out = String::with_capacity(snake.len());
    let mut upper_next = false;
    for c in snake.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.push(c.to_ascii_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_word_prefix_renders_unchanged() {
        let prefix = RootPrefix::from_snake("sample").expect("sample is valid");
        assert_eq!(prefix.as_str(), "sample");
        assert_eq!(prefix.snake(), "sample");
    }

    #[test]
    fn a_multi_word_prefix_renders_lower_camel() {
        let prefix = RootPrefix::from_snake("local_indexer").expect("local_indexer is valid");
        assert_eq!(prefix.as_str(), "localIndexer");
        assert_eq!(prefix.snake(), "local_indexer");
    }

    #[test]
    fn an_empty_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake(""),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn an_upper_first_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake("Sample"),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn a_leading_underscore_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake("_x"),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn a_double_underscore_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake("x__y"),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn a_trailing_underscore_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake("x_"),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn a_digit_first_prefix_is_refused() {
        assert!(matches!(
            RootPrefix::from_snake("9x"),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn a_prefix_over_forty_characters_is_refused() {
        let long: &'static str = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddz";
        assert_eq!(long.len(), 41);
        assert!(matches!(
            RootPrefix::from_snake(long),
            Err(EngineError::RootPrefixInvalid { .. })
        ));
    }

    #[test]
    fn owns_is_prefix_plus_one_uppercase_letter_plus_any_tail() {
        let prefix = RootPrefix::from_snake("local_indexer").unwrap();
        assert!(prefix.owns("localIndexerGetContext"));
        assert!(prefix.owns("localIndexerX"));
        assert!(!prefix.owns("localIndexer"));
        assert!(!prefix.owns("localindexerX"));
        assert!(!prefix.owns("localIndexer2"));
    }
}
