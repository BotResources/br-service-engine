mod verdict;

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::nats::{Nats, NatsCondition, NatsHealth, RelayHealth, RelayHealthReceiver};
use crate::readiness::{Readiness, ReadinessHandle};
use tokio::sync::watch;

use crate::boot::REASON_MIRRORS;
use crate::housekeeping::health::RelaysHealthReceiver;
use crate::housekeeping::mirror::MirrorsHealthReceiver;
use crate::observe::{
    DEP_INBOUND, DEP_LISTENER, DEP_MIRRORS, DEP_NATS, DEP_POSTGRES, record_dependency,
};

use verdict::verdict;
pub use verdict::{
    REASON_INBOUND_STOPPED, REASON_NATS_UNREACHABLE, REASON_RELAY_DEGRADED, REASON_SHUTTING_DOWN,
    REASON_WORKER_STOPPED,
};

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
    inbound: Option<watch::Receiver<bool>>,
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
            inbound: None,
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

    pub fn with_inbound_health(mut self, inbound: watch::Receiver<bool>) -> Self {
        self.inbound = Some(inbound);
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
        let inbound_up = self.inbound.as_ref().map(|rx| *rx.borrow()).unwrap_or(true);
        let nats = self.nats.as_ref().map(NatsProbe::sample);
        let mirrors = self.mirrors.borrow().clone();
        record_dependency(DEP_POSTGRES, true);
        record_dependency(DEP_LISTENER, listener_up != Some(false));
        record_dependency(DEP_MIRRORS, mirrors.converged());
        if self.inbound.is_some() {
            record_dependency(DEP_INBOUND, inbound_up);
        }
        if let Some(nats) = nats {
            record_dependency(DEP_NATS, nats.is_up());
        }
        verdict(
            listener_up,
            nats,
            inbound_up,
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
