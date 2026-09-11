use br_core_integration::{Actor, ServiceAccountId};
use uuid::Uuid;

pub(crate) const SERVICE_ACTOR_NAMESPACE: Uuid =
    Uuid::from_u128(0x6f3a_1c8e_4b27_4d59_9e10_a3f2_77c5_8d41);

pub(crate) fn service_actor(service: &str) -> Actor {
    let id = Uuid::new_v5(&SERVICE_ACTOR_NAMESPACE, service.as_bytes());
    Actor::Service(ServiceAccountId::from(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_service_resolves_to_a_deterministic_v5_service_identity() {
        let actor = service_actor("project");
        assert!(actor.is_service());
        assert_eq!(actor, service_actor("project"));
        assert_ne!(actor, service_actor("billing"));
    }

    #[test]
    fn the_service_actor_id_is_frozen_to_the_shared_v5_namespace() {
        let expected = ServiceAccountId::from(
            Uuid::parse_str("9a350fca-0614-5e1a-8fde-c928e548ed7f").unwrap(),
        );
        assert_eq!(service_actor("project"), Actor::Service(expected));
    }
}
