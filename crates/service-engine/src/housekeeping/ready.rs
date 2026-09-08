use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::nats::{Nats, NatsCondition, NatsHealth, RelayHealth, RelayHealthReceiver};
use br_util_axum_readiness::{Readiness, ReadinessHandle};
use tokio::sync::watch;

use crate::boot::{REASON_LISTEN_FAILED, REASON_MIRRORS};
use crate::housekeeping::health::{RelaysHealth, RelaysHealthReceiver};
use crate::housekeeping::mirror::{MirrorsHealth, MirrorsHealthReceiver};
use crate::observe::{
    DEP_LISTENER, DEP_MIRRORS, DEP_NATS, DEP_POSTGRES, record_dependency,
};

pub const REASON_RELAY_DEGRADED: &str = "a relay is not draining";
pub const REASON_WORKER_STOPPED: &str = "a background worker stopped";
pub const REASON_NATS_UNREACHABLE: &str = "nats has been unreachable past its grace window";

struct NatsProbe {
    nats: Nats,
    health: Mutex<NatsHealth>,
}

impl NatsProbe {
    fn sample(&self) -> NatsCondition {
        let reachable = self.nats.reachable();
        self.health
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .observe(reachable, Instant::now())
    }
}

pub struct ReadinessAssembly {
    handle: ReadinessHandle,
    mirrors: MirrorsHealthReceiver,
    relays: Option<RelaysHealthReceiver>,
    fabric: Vec<RelayHealthReceiver>,
    listener: Option<watch::Receiver<bool>>,
    nats: Option<NatsProbe>,
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
            nats: None,
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

    pub fn with_nats(mut self, nats: Nats, grace: Duration) -> Self {
        self.nats = Some(NatsProbe {
            nats,
            health: Mutex::new(NatsHealth::new(grace)),
        });
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
        let nats = self.nats.as_ref().map(NatsProbe::sample);
        let mirrors = self.mirrors.borrow().clone();
        record_dependency(DEP_POSTGRES, true);
        record_dependency(DEP_LISTENER, listener_up != Some(false));
        record_dependency(DEP_MIRRORS, mirrors.converged());
        if let Some(nats) = nats {
            record_dependency(DEP_NATS, nats.is_up());
        }
        verdict(
            listener_up,
            nats,
            &mirrors,
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
            .field("nats", &self.nats.is_some())
            .finish_non_exhaustive()
    }
}

fn verdict(
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
