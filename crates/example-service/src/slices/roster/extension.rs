//! Reference for the project-specific extension pattern: a shared core the
//! producer publishes ([`example_contract::PublishedPerson`]) plus an extension
//! this project owns ([`PersonRole`]), composed with the engine's
//! [`Extended`](service_engine::Extended) as the second type parameter. Because
//! the extension is typed, a value carrying a role this project does not model
//! is **denied** at deserialization — never mirrored as opaque JSON. Another
//! project reading the same core names a different extension and sees only its
//! own roles.
//!
//! It is a documented reference, not a live mirror: registering it would need a
//! producer publishing the extended shape. The deny path is proven by the
//! tests below, which is the whole of the law.

use example_contract::PublishedPerson;
use serde::{Deserialize, Serialize};
use service_engine::{Consumed, Extended};

/// The project-owned extension, externally tagged: an unknown tag fails to
/// deserialize, which is the deny.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersonRole {
    Employee { team: String },
    Contractor { agency: String },
}

/// The consumed value: the producer's core plus this project's role. Wrapped in
/// a newtype so this crate may implement `Consumed` for it (the core type is
/// foreign, so a bare `Extended<PublishedPerson, _>` could not carry the impl).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExtendedPerson(pub Extended<PublishedPerson, PersonRole>);

impl Consumed for ExtendedPerson {
    const PREFIX: &'static str = "exampletwin/persons-extended/v1/";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn wire(role: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "id": Uuid::now_v7(),
            "email": "a@example.test",
            "display_name": "A",
            "extension": role,
        })
    }

    #[test]
    fn a_role_this_project_models_deserializes() {
        let value: ExtendedPerson =
            serde_json::from_value(wire(serde_json::json!({ "Employee": { "team": "platform" } })))
                .expect("a known role");
        assert_eq!(
            value.0.extension,
            PersonRole::Employee {
                team: "platform".to_string()
            }
        );
    }

    #[test]
    fn a_role_this_project_does_not_model_is_denied() {
        let denied: Result<ExtendedPerson, _> =
            serde_json::from_value(wire(serde_json::json!({ "Volunteer": { "cause": "x" } })));
        assert!(denied.is_err(), "an unknown extension must be denied, not mirrored");
    }
}
