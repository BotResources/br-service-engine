use br_util_axum_readiness::{Readiness, ReadinessHandle};
use br_util_nats_fabric::{RelayHealth, RelayHealthReceiver};
use tokio::sync::watch;

use crate::boot::{REASON_LISTEN_FAILED, REASON_MIRRORS};
use crate::housekeeping::health::{RelaysHealth, RelaysHealthReceiver};
use crate::housekeeping::mirror::{MirrorsHealth, MirrorsHealthReceiver};

pub const REASON_RELAY_DEGRADED: &str = "a relay is not draining";
pub const REASON_WORKER_STOPPED: &str = "a background worker stopped";

pub struct ReadinessAssembly {
    handle: ReadinessHandle,
    mirrors: MirrorsHealthReceiver,
    relays: Option<RelaysHealthReceiver>,
    fabric: Vec<RelayHealthReceiver>,
    listener: Option<watch::Receiver<bool>>,
}

impl ReadinessAssembly {
    pub fn new(handle: ReadinessHandle, mirrors: MirrorsHealthReceiver) -> Self {
        handle.set_not_ready(REASON_MIRRORS);
        Self {
            handle,
            mirrors,
            relays: None,
            fabric: Vec::new(),
            listener: None,
        }
    }

    pub fn with_relays(mut self, relays: RelaysHealthReceiver) -> Self {
        self.relays = Some(relays);
        self
    }

    pub fn with_listener(mut self, listener: watch::Receiver<bool>) -> Self {
        self.listener = Some(listener);
        self
    }

    pub fn watching_fabric_relay(mut self, health: RelayHealthReceiver) -> Self {
        self.fabric.push(health);
        self
    }

    pub fn handle(&self) -> &ReadinessHandle {
        &self.handle
    }

    pub fn verdict(&self) -> Option<&'static str> {
        let fabric: Vec<RelayHealth> = self
            .fabric
            .iter()
            .map(|health| health.borrow().clone())
            .collect();
        let listener_up = self.listener.as_ref().map(|rx| *rx.borrow());
        verdict(
            listener_up,
            &self.mirrors.borrow().clone(),
            self.relays.as_ref().map(|r| r.borrow().clone()).as_ref(),
            &fabric,
        )
    }

    pub fn refresh(&self) -> Readiness {
        match self.verdict() {
            None => {
                self.handle.set_ready();
                Readiness::Ready
            }
            Some(reason) => {
                self.handle.set_not_ready(reason);
                Readiness::NotReady {
                    reason: reason.to_string(),
                }
            }
        }
    }
}

impl std::fmt::Debug for ReadinessAssembly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadinessAssembly")
            .field("mirrors", &self.mirrors.borrow().len())
            .field("relays", &self.relays.is_some())
            .field("fabric_relays", &self.fabric.len())
            .finish_non_exhaustive()
    }
}

fn verdict(
    listener_up: Option<bool>,
    mirrors: &MirrorsHealth,
    relays: Option<&RelaysHealth>,
    fabric: &[RelayHealth],
) -> Option<&'static str> {
    if listener_up == Some(false) {
        return Some(REASON_LISTEN_FAILED);
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
    use crate::name::{MirrorName, RelayName};
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
        assert_eq!(verdict(None, &MirrorsHealth::default(), None, &[]), None);
        assert_eq!(
            verdict(Some(true), &MirrorsHealth::default(), None, &[]),
            None
        );
    }

    #[test]
    fn a_listener_that_dropped_at_runtime_takes_the_pod_out_of_rotation_before_anything_else() {
        assert_eq!(
            verdict(Some(false), &converged(), Some(&backing_off()), &[]),
            Some(REASON_LISTEN_FAILED),
            "a pod that has lost its LISTEN is blind and must go DOWN even if it once was ready"
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
            verdict(None, &converging(), Some(&backing_off()), &[]),
            Some(REASON_MIRRORS),
            "the boot order is mirrors first, so the reason names the earliest unmet condition"
        );
    }

    #[test]
    fn a_relay_backing_off_takes_a_converged_service_back_out_of_rotation() {
        assert_eq!(
            verdict(None, &converged(), Some(&backing_off()), &[]),
            Some(REASON_RELAY_DEGRADED)
        );
        assert_eq!(verdict(None, &converged(), None, &[]), None);
    }

    #[test]
    fn a_degraded_hosted_fabric_relay_is_read_even_though_the_engine_board_is_clean() {
        let degraded = RelayHealth::Degraded {
            reason: br_util_nats_fabric::REASON_NO_STREAM,
        };
        assert_eq!(
            verdict(
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
                &converged(),
                Some(&RelaysHealth::default()),
                &[RelayHealth::Healthy]
            ),
            None
        );
    }

    #[test]
    fn no_readiness_reason_leaks_the_name_of_what_failed() {
        for reason in [REASON_MIRRORS, REASON_RELAY_DEGRADED] {
            assert!(!reason.contains("directory"));
            assert!(!reason.contains("integration_outbox"));
            assert!(!reason.contains("no such stream"));
        }
    }
}
