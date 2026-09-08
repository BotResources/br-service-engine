use crate::boot::{REASON_LISTEN_FAILED, REASON_MIRRORS};
use crate::housekeeping::health::RelaysHealth;
use crate::housekeeping::mirror::MirrorsHealth;
use crate::nats::{NatsCondition, RelayHealth};

pub const REASON_RELAY_DEGRADED: &str = "a relay is not draining";
pub const REASON_WORKER_STOPPED: &str = "a background worker stopped";
pub const REASON_NATS_UNREACHABLE: &str = "nats has been unreachable past its grace window";

pub(crate) fn verdict(
    listener_up: Option<bool>,
    nats: Option<NatsCondition>,
    mirrors: &MirrorsHealth,
    relays: Option<&RelaysHealth>,
    fabric: &[RelayHealth],
) -> Option<&'static str> {
    if listener_up == Some(false) {
        return Some(REASON_LISTEN_FAILED);
    }
    if nats.is_some_and(NatsCondition::holds_readiness) {
        return Some(REASON_NATS_UNREACHABLE);
    }
    if !mirrors.converged() {
        return Some(REASON_MIRRORS);
    }
    if relays.is_some_and(|board| !board.is_healthy()) {
        return Some(REASON_RELAY_DEGRADED);
    }
    if fabric.iter().any(|health| *health != RelayHealth::Healthy) {
        return Some(REASON_RELAY_DEGRADED);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::housekeeping::health::RelayCondition;
    use crate::housekeeping::mirror::MirrorCondition;
    use crate::housekeeping::ready::ReadinessAssembly;
    use crate::name::{MirrorName, RelayName};
    use br_util_axum_readiness::ReadinessHandle;
    use std::time::Duration;

    fn converged() -> MirrorsHealth {
        MirrorsHealth::from_iter([(
            MirrorName::from_static("directory"),
            MirrorCondition::Converged,
        )])
    }

    fn converging() -> MirrorsHealth {
        MirrorsHealth::from_iter([(
            MirrorName::from_static("directory"),
            MirrorCondition::Converging,
        )])
    }

    fn backing_off() -> RelaysHealth {
        RelaysHealth::from_iter([(
            RelayName::from_static("integration_outbox"),
            RelayCondition::BackingOff {
                attempts: 1,
                retry_in: Duration::from_millis(500),
                reason: "publishing a claimed row: no such stream".to_string(),
            },
        )])
    }

    #[test]
    fn a_service_that_mirrors_nothing_and_relays_nothing_is_ready() {
        assert_eq!(
            verdict(None, None, &MirrorsHealth::default(), None, &[]),
            None
        );
        assert_eq!(
            verdict(Some(true), None, &MirrorsHealth::default(), None, &[]),
            None
        );
    }

    #[test]
    fn a_listener_that_dropped_at_runtime_takes_the_pod_out_of_rotation_before_anything_else() {
        assert_eq!(
            verdict(
                Some(false),
                Some(NatsCondition::PastGrace),
                &converged(),
                Some(&backing_off()),
                &[]
            ),
            Some(REASON_LISTEN_FAILED),
            "a pod that has lost its LISTEN is blind and must go DOWN even if it once was ready"
        );
    }

    #[test]
    fn nats_within_its_grace_window_keeps_a_converged_pod_up() {
        assert_eq!(
            verdict(
                Some(true),
                Some(NatsCondition::WithinGrace),
                &converged(),
                None,
                &[]
            ),
            None,
            "a blink shorter than nats_grace never takes the pod out of rotation"
        );
    }

    #[test]
    fn nats_past_its_grace_window_takes_the_pod_out_of_rotation_after_the_listener() {
        assert_eq!(
            verdict(
                Some(true),
                Some(NatsCondition::PastGrace),
                &converging(),
                None,
                &[]
            ),
            Some(REASON_NATS_UNREACHABLE),
            "a real nats outage goes DOWN loudly, and before the mirrors it also feeds"
        );
    }

    #[test]
    fn a_service_cannot_assemble_readiness_without_naming_the_mirror_board_it_must_wait_on() {
        let supervisor = crate::housekeeping::mirror::MirrorSupervisor::new();
        let assembly = ReadinessAssembly::new(ReadinessHandle::ready(), supervisor.health());
        assert_eq!(
            assembly.verdict(),
            None,
            "an empty board is converged, so a service with no mirror still becomes ready; the \
             board is a required argument so a service that has one cannot forget to wait on it"
        );
    }

    #[test]
    fn a_mirror_that_has_not_converged_holds_readiness_down_before_any_relay_is_considered() {
        assert_eq!(
            verdict(None, None, &converging(), Some(&backing_off()), &[]),
            Some(REASON_MIRRORS),
            "the boot order is mirrors first, so the reason names the earliest unmet condition"
        );
    }

    #[test]
    fn a_relay_backing_off_takes_a_converged_service_back_out_of_rotation() {
        assert_eq!(
            verdict(None, None, &converged(), Some(&backing_off()), &[]),
            Some(REASON_RELAY_DEGRADED)
        );
        assert_eq!(verdict(None, None, &converged(), None, &[]), None);
    }

    #[test]
    fn a_degraded_hosted_fabric_relay_is_read_even_though_the_engine_board_is_clean() {
        let degraded = RelayHealth::Degraded {
            reason: crate::nats::REASON_NO_STREAM,
        };
        assert_eq!(
            verdict(
                None,
                None,
                &converged(),
                Some(&RelaysHealth::default()),
                &[degraded]
            ),
            Some(REASON_RELAY_DEGRADED),
            "the hosted relay keeps its own board; a clean engine board is not proof it publishes"
        );
        assert_eq!(
            verdict(
                None,
                None,
                &converged(),
                Some(&RelaysHealth::default()),
                &[RelayHealth::Healthy]
            ),
            None
        );
    }

    #[test]
    fn no_readiness_reason_leaks_the_name_of_what_failed() {
        for reason in [
            REASON_MIRRORS,
            REASON_RELAY_DEGRADED,
            REASON_NATS_UNREACHABLE,
        ] {
            assert!(!reason.contains("directory"));
            assert!(!reason.contains("integration_outbox"));
            assert!(!reason.contains("no such stream"));
        }
    }
}
