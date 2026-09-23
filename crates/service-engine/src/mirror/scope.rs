use crate::nats::{KvKey, KvKeyError, KvPrefix};

const SEPARATORS: [char; 2] = ['/', '.'];
const JOINERS: [char; 2] = ['_', '-'];
const MANIFEST_SUFFIX: &str = "_manifest";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyScope {
    Prefix { prefix: KvPrefix, manifest: KvKey },
    Key(KvKey),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ScopeRefusal {
    #[error("is not a valid key: {0}")]
    Invalid(#[from] KvKeyError),

    #[error("starts with a separator, so its first segment is empty")]
    LeadingSeparator,

    #[error("holds an empty segment (two separators in a row)")]
    EmptySegment,

    #[error(
        "ends mid-segment on {0:?}: it is neither one whole key nor a prefix ending in '/' or '.'"
    )]
    MidSegment(char),

    #[error(
        "ends in {MANIFEST_SUFFIX}, the name the engine gives an offer manifest, so it would read \
         a manifest as data"
    )]
    ManifestName,
}

impl KeyScope {
    pub(crate) fn parse(declared: &str) -> Result<Self, ScopeRefusal> {
        let key = KvKey::new(declared)?;
        if declared.starts_with(SEPARATORS) {
            return Err(ScopeRefusal::LeadingSeparator);
        }
        let mut previous_is_separator = false;
        for c in declared.chars() {
            let is_separator = SEPARATORS.contains(&c);
            if is_separator && previous_is_separator {
                return Err(ScopeRefusal::EmptySegment);
            }
            previous_is_separator = is_separator;
        }
        if let Some(base) = declared.strip_suffix(SEPARATORS) {
            return Ok(Self::Prefix {
                prefix: KvPrefix::new(declared)?,
                manifest: KvKey::new(format!("{base}{MANIFEST_SUFFIX}"))?,
            });
        }
        if let Some(joiner) = declared.chars().last().filter(|c| JOINERS.contains(c)) {
            return Err(ScopeRefusal::MidSegment(joiner));
        }
        if declared.ends_with(MANIFEST_SUFFIX) {
            return Err(ScopeRefusal::ManifestName);
        }
        Ok(Self::Key(key))
    }

    pub(crate) fn matches(&self, key: &str) -> bool {
        match self {
            Self::Prefix { prefix, .. } => prefix.matches(key),
            Self::Key(exact) => exact.as_str() == key,
        }
    }

    pub(crate) fn manifest(&self) -> Option<&KvKey> {
        match self {
            Self::Prefix { manifest, .. } => Some(manifest),
            Self::Key(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prefix(declared: &str) -> (KvPrefix, KvKey) {
        match KeyScope::parse(declared).expect("a separator-terminated prefix is accepted") {
            KeyScope::Prefix { prefix, manifest } => (prefix, manifest),
            KeyScope::Key(key) => panic!("{declared:?} parsed as the single key {key:?}"),
        }
    }

    #[test]
    fn a_slash_terminated_prefix_keeps_its_manifest_where_0_3_0_put_it() {
        let (prefix, manifest) = prefix("catalog/items/");
        assert_eq!(prefix.as_str(), "catalog/items/");
        assert_eq!(manifest.as_str(), "catalog/items_manifest");
    }

    #[test]
    fn a_dot_terminated_prefix_is_a_prefix_with_a_sibling_manifest() {
        let (prefix, manifest) = prefix("catalog.item.");
        assert_eq!(prefix.as_str(), "catalog.item.");
        assert_eq!(manifest.as_str(), "catalog.item_manifest");
    }

    #[test]
    fn a_manifest_never_sits_under_its_own_prefix_whatever_the_separator() {
        for declared in [
            "a/", "a.", "a/b/", "a.b.", "a/b.", "a.b/", "a_b-c/", "a_b-c.",
        ] {
            let scope = KeyScope::parse(declared).expect("accepted");
            let manifest = scope.manifest().expect("a prefix has a manifest");
            assert!(
                !scope.matches(manifest.as_str()),
                "the manifest {manifest:?} of {declared:?} would be read as data"
            );
        }
    }

    #[test]
    fn a_prefix_matches_every_key_beneath_it_and_nothing_that_only_shares_its_characters() {
        let scope = KeyScope::parse("catalog.item.").unwrap();
        assert!(scope.matches("catalog.item.1"));
        assert!(scope.matches("catalog.item.1.part"));
        assert!(!scope.matches("catalog.items.1"));
        assert!(!scope.matches("catalog.item"));
        assert!(!scope.matches("catalog.item_manifest"));
    }

    #[test]
    fn a_string_without_a_trailing_separator_is_one_exact_key_with_no_manifest() {
        for declared in ["catalog.settings", "catalog/_meta", "settings", "a-b_c.d"] {
            let scope = KeyScope::parse(declared).expect("a whole key is accepted");
            assert_eq!(
                scope,
                KeyScope::Key(KvKey::new(declared).unwrap()),
                "{declared:?} is one key"
            );
            assert_eq!(
                scope.manifest(),
                None,
                "a single key is judged against no manifest"
            );
        }
    }

    #[test]
    fn an_exact_key_matches_itself_only() {
        let scope = KeyScope::parse("catalog.settings").unwrap();
        assert!(scope.matches("catalog.settings"));
        assert!(!scope.matches("catalog.settings.extra"));
        assert!(!scope.matches("catalog.settings/extra"));
        assert!(!scope.matches("catalog.settingsx"));
        assert!(!scope.matches("catalog.setting"));
        assert!(!scope.matches("catalog.settings_manifest"));
    }

    #[test]
    fn a_string_cut_mid_segment_is_refused_as_ambiguous() {
        assert_eq!(
            KeyScope::parse("catalog.item_"),
            Err(ScopeRefusal::MidSegment('_'))
        );
        assert_eq!(
            KeyScope::parse("catalog/item-"),
            Err(ScopeRefusal::MidSegment('-'))
        );
    }

    #[test]
    fn an_exact_key_named_like_a_manifest_is_refused() {
        assert_eq!(
            KeyScope::parse("catalog/items_manifest"),
            Err(ScopeRefusal::ManifestName)
        );
        assert_eq!(
            KeyScope::parse("catalog.item_manifest"),
            Err(ScopeRefusal::ManifestName)
        );
    }

    #[test]
    fn an_empty_segment_is_refused() {
        for declared in [
            "catalog//",
            "catalog..",
            "catalog./",
            "catalog/.",
            "a//b",
            "a..b",
        ] {
            assert_eq!(
                KeyScope::parse(declared),
                Err(ScopeRefusal::EmptySegment),
                "{declared:?}"
            );
        }
    }

    #[test]
    fn a_leading_separator_is_refused() {
        for declared in ["/", ".", "/catalog/", ".catalog."] {
            assert_eq!(
                KeyScope::parse(declared),
                Err(ScopeRefusal::LeadingSeparator),
                "{declared:?}"
            );
        }
    }

    #[test]
    fn an_empty_or_malformed_string_is_refused() {
        assert_eq!(
            KeyScope::parse(""),
            Err(ScopeRefusal::Invalid(KvKeyError::Empty))
        );
        assert!(matches!(
            KeyScope::parse("has space/"),
            Err(ScopeRefusal::Invalid(KvKeyError::InvalidChar { .. }))
        ));
    }
}
