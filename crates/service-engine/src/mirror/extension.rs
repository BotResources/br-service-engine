//! The project-specific extension pattern for a consumed value: a shared `core`
//! published by the producer, and a project-owned `extension` given as the
//! second type parameter. Both are typed, so a value carrying an extension this
//! project does not model fails to deserialize — an unknown extension is
//! **denied**, never mirrored as opaque JSON. A different project reading the
//! same offer names a different extension type; each sees only the extensions it
//! declares.
//!
//! The core is flattened onto the value, so the producer may add a core field
//! without breaking a consumer (the additive rule). The extension is a named
//! object typed by the project — model it as an externally-tagged enum and an
//! unknown tag is the deny.

use serde::{Deserialize, Serialize};

/// A consumed value split into a producer's shared `core` and a project-owned
/// `extension`. A consumer makes it a [`Consumed`](super::consumed::Consumed) by
/// implementing that trait on the concrete `Extended<Core, Ext>` it reads (the
/// prefix and version live there), so two projects extending the same core with
/// different `Ext` types are two distinct, independently-typed consumptions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extended<Core, Ext> {
    #[serde(flatten)]
    pub core: Core,
    pub extension: Ext,
}

impl<Core, Ext> Extended<Core, Ext> {
    pub fn new(core: Core, extension: Ext) -> Self {
        Self { core, extension }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct PersonCore {
        id: u32,
        email: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum Role {
        Employee { team: String },
        Contractor { agency: String },
    }

    type ExtendedPerson = Extended<PersonCore, Role>;

    #[test]
    fn a_known_extension_deserializes_alongside_the_core() {
        let wire = serde_json::json!({
            "id": 7,
            "email": "a@example.test",
            "extension": { "Employee": { "team": "platform" } }
        });
        let value: ExtendedPerson = serde_json::from_value(wire).expect("a known extension");
        assert_eq!(value.core.id, 7);
        assert_eq!(
            value.extension,
            Role::Employee {
                team: "platform".to_string()
            }
        );
    }

    #[test]
    fn an_unknown_extension_is_denied() {
        let wire = serde_json::json!({
            "id": 7,
            "email": "a@example.test",
            "extension": { "Volunteer": { "cause": "x" } }
        });
        let denied: Result<ExtendedPerson, _> = serde_json::from_value(wire);
        assert!(
            denied.is_err(),
            "an extension this project does not model must be denied"
        );
    }

    #[test]
    fn an_added_core_field_is_ignored_not_denied() {
        let wire = serde_json::json!({
            "id": 7,
            "email": "a@example.test",
            "added_later": true,
            "extension": { "Contractor": { "agency": "acme" } }
        });
        let value: ExtendedPerson = serde_json::from_value(wire).expect("core is additive");
        assert_eq!(
            value.extension,
            Role::Contractor {
                agency: "acme".to_string()
            }
        );
    }
}
