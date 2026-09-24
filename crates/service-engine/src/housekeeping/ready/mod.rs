mod verdict;

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::boot::REASON_MIRRORS;
use crate::housekeeping::health::RelaysHealthReceiver;
use crate::housekeeping::mirror::{MirrorsHealth, MirrorsHealthReceiver};
use crate::nats::{Nats, NatsCondition, NatsHealth, RelayHealth, RelayHealthReceiver};
use crate::observe::{
    DEP_INBOUND, DEP_LISTENER, DEP_MIRRORS, DEP_NATS, DEP_POSTGRES, record_dependency,
};
use crate::readiness::{Readiness, ReadinessHandle};

use verdict::verdict;
pub use verdict::{
    REASON_INBOUND_STOPPED, REASON_NATS_UNREACHABLE, REASON_RELAY_DEGRADED, REASON_REQUIRED_KEYS,
    REASON_SCHEMA_VERSION_DISPLACED, REASON_SHUTTING_DOWN, REASON_WORKER_STOPPED,
};

const DEP_REQUIRED_KEYS: &str = "required_keys";

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
    schema_displaced: Option<watch::Receiver<bool>>,
    required_keys: Vec<watch::Receiver<Vec<String>>>,
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
            schema_displaced: None,
            required_keys: Vec::new(),
        }
    }

    pub fn with_schema_version(mut self, displaced: watch::Receiver<bool>) -> Self {
        self.schema_displaced = Some(displaced);
        self
    }

    pub fn with_required_keys(mut self, required_keys: Vec<watch::Receiver<Vec<String>>>) -> Self {
        self.required_keys = required_keys;
        self
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
        self.assess().0
    }

    fn missing_required_keys(&self) -> Vec<String> {
        self.required_keys
            .iter()
            .flat_map(|rx| rx.borrow().clone())
            .collect()
    }

    fn assess(&self) -> (Option<&'static str>, MirrorsHealth, Vec<String>) {
        if self
            .schema_displaced
            .as_ref()
            .is_some_and(|rx| *rx.borrow())
        {
            return (
                Some(REASON_SCHEMA_VERSION_DISPLACED),
                self.mirrors.borrow().clone(),
                Vec::new(),
            );
        }
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
        let base = verdict(
            listener_up,
            nats,
            inbound_up,
            &mirrors,
            self.relays.as_ref().map(|r| r.borrow().clone()).as_ref(),
            &fabric,
        );
        let missing = self.missing_required_keys();
        if !self.required_keys.is_empty() {
            record_dependency(DEP_REQUIRED_KEYS, missing.is_empty());
        }
        let verdict = base.or_else(|| (!missing.is_empty()).then_some(REASON_REQUIRED_KEYS));
        (verdict, mirrors, missing)
    }

    pub fn refresh(&self) -> Readiness {
        let (verdict, mirrors, missing) = self.assess();
        match verdict {
            None => {
                self.handle.set_ready();
                Readiness::Ready
            }
            Some(reason) => {
                let reason = detailed(reason, &mirrors, &missing);
                self.handle.set_not_ready(reason.clone());
                Readiness::NotReady { reason }
            }
        }
    }
}

fn detailed(reason: &'static str, mirrors: &MirrorsHealth, missing: &[String]) -> String {
    let mut detailed = reason.to_string();
    if reason == REASON_MIRRORS {
        for (name, condition) in mirrors.iter() {
            if let Some(detail) = condition.reason() {
                detailed.push_str(&format!("; {name}: {detail}"));
            }
        }
    }
    if reason == REASON_REQUIRED_KEYS {
        for key in missing {
            detailed.push_str(&format!("; {key}"));
        }
    }
    detailed
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
