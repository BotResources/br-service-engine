#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum KvKeyError {
    #[error("kv key is empty")]
    Empty,
    #[error("kv key {value:?} contains a character outside [A-Za-z0-9_./-]")]
    InvalidChar { value: String },
}

fn valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/')
}

fn validate(value: &str) -> Result<(), KvKeyError> {
    if value.is_empty() {
        return Err(KvKeyError::Empty);
    }
    if value.chars().any(|c| !valid_char(c)) {
        return Err(KvKeyError::InvalidChar {
            value: value.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KvKey(String);

impl KvKey {
    pub fn new(value: impl Into<String>) -> Result<Self, KvKeyError> {
        let value = value.into();
        validate(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for KvKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPrefix(String);

impl KvPrefix {
    pub fn new(value: impl Into<String>) -> Result<Self, KvKeyError> {
        let value = value.into();
        validate(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn watch_subject(&self) -> String {
        format!("{}>", self.0)
    }

    pub fn matches(&self, key: &str) -> bool {
        key.starts_with(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_slash_delimited_key() {
        assert_eq!(
            KvKey::new("identity/users/abc").unwrap().as_str(),
            "identity/users/abc"
        );
    }

    #[test]
    fn rejects_an_empty_key() {
        assert_eq!(KvKey::new(""), Err(KvKeyError::Empty));
    }

    #[test]
    fn rejects_a_space() {
        assert!(matches!(
            KvKey::new("has space"),
            Err(KvKeyError::InvalidChar { .. })
        ));
    }

    #[test]
    fn a_prefix_watch_subject_ends_in_a_wildcard() {
        assert_eq!(
            KvPrefix::new("identity/users/").unwrap().watch_subject(),
            "identity/users/>"
        );
    }
}
