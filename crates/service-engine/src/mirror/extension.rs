use serde::{Deserialize, Serialize};

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
