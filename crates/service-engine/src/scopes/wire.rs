use br_core_integration::{
    Actor, CommandCoords, EventMetadata, IntegrationCommand, ServiceAccountId,
};
use br_core_scope::{DeclareServiceScopes, ScopeDeclaration, ServiceKey};
use br_scope_declaration_contract::{
    VERSION, accepted_event_coords, command_type, declare_command_coords, rejected_event_coords,
};
use chrono::Utc;
use uuid::Uuid;

use crate::nats::event_subject;

const DECLARING_SERVICE_NAMESPACE: Uuid =
    Uuid::from_u128(0x6f3a_1c8e_4b27_4d59_9e10_a3f2_77c5_8d41);

const CONTRACT: &str = "the frozen scope-declaration contract renders valid coordinates";

pub(super) struct ConfirmationSubjects {
    pub(super) accepted: String,
    pub(super) rejected: String,
}

impl ConfirmationSubjects {
    pub(super) fn render() -> Self {
        Self {
            accepted: event_subject(&accepted_event_coords().expect(CONTRACT)),
            rejected: event_subject(&rejected_event_coords().expect(CONTRACT)),
        }
    }
}

pub(super) fn command_subject() -> String {
    render_command_subject(&declare_command_coords().expect(CONTRACT))
}

pub(super) fn build_command(
    service: &ServiceKey,
    correlation_id: Uuid,
    declaration: ScopeDeclaration,
) -> IntegrationCommand<DeclareServiceScopes> {
    IntegrationCommand::new(
        Uuid::now_v7(),
        command_type(),
        VERSION,
        Utc::now(),
        EventMetadata::new(declaring_actor(service), correlation_id),
        DeclareServiceScopes::new(declaration),
    )
}

pub(super) fn declaring_actor(service: &ServiceKey) -> Actor {
    let id = Uuid::new_v5(&DECLARING_SERVICE_NAMESPACE, service.as_str().as_bytes());
    Actor::Service(ServiceAccountId::from(id))
}

fn render_command_subject(coords: &CommandCoords) -> String {
    format!(
        "integration.cmd.{}.{}.{}.v{}",
        coords.receiver.as_str(),
        coords.aggregate.as_str(),
        coords.verb.as_str(),
        coords.version
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_confirmation_subjects_are_the_two_frozen_event_subjects() {
        let subjects = ConfirmationSubjects::render();
        assert_eq!(
            subjects.accepted,
            "integration.evt.identity.service_scope.accepted.v1"
        );
        assert_eq!(
            subjects.rejected,
            "integration.evt.identity.service_scope.rejected.v1"
        );
    }

    #[test]
    fn the_declare_command_renders_the_frozen_command_subject() {
        assert_eq!(
            command_subject(),
            "integration.cmd.identity.service_scope.declare.v1"
        );
    }

    #[test]
    fn the_declaring_actor_is_a_deterministic_v5_service_identity() {
        let service = ServiceKey::new("project").unwrap();
        let actor = declaring_actor(&service);
        assert!(actor.is_service());
        assert_eq!(actor, declaring_actor(&service));
        assert_ne!(actor, declaring_actor(&ServiceKey::new("billing").unwrap()));
    }

    #[test]
    fn the_declaring_actor_id_is_frozen_to_the_shared_v5_namespace() {
        let expected = ServiceAccountId::from(
            Uuid::parse_str("9a350fca-0614-5e1a-8fde-c928e548ed7f").unwrap(),
        );
        assert_eq!(
            declaring_actor(&ServiceKey::new("project").unwrap()),
            Actor::Service(expected)
        );
    }

    #[test]
    fn the_command_carries_the_correlation_id_and_the_frozen_type() {
        let service = ServiceKey::new("project").unwrap();
        let correlation_id = Uuid::now_v7();
        let declaration = crate::scopes::ScopeManifest::of(&[&["project:read"]])
            .declaration()
            .unwrap();
        let command = build_command(&service, correlation_id, declaration);
        assert_eq!(command.command_type, "service_scope.declare");
        assert_eq!(command.version, 1);
        assert_eq!(command.metadata.correlation_id, correlation_id);
    }
}
