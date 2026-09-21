use example_contract::PublishedPerson;
use serde::{Deserialize, Serialize};
use service_engine::{Consumed, Extended};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersonRole {
    Employee { team: String },
    Contractor { agency: String },
}

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
