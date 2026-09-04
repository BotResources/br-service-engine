use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::name::NounName;
use service_engine::projector::Emission;

use crate::sample::gate::Gate;
use crate::sample::spy::SpyAssignments;
use crate::sample::spy::recorder::Spy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowMode {
    Keys,
    OrderedHead(i64),
    LiveQuery,
    QueryThenEmpty,
    MembershipQuery,
    MembershipOnlyQuery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CohortMode {
    PerPrincipal,
    PerTenant,
}

impl SpyAssignments {
    pub fn new(spy: Arc<Spy>) -> Self {
        Self {
            spy,
            window: WindowMode::Keys,
            cohort: CohortMode::PerPrincipal,
            emission: Emission::Coalesced,
            dims: Dims::EMPTY,
            also: None,
            gate: None,
            load_gate: None,
            fail_switch: None,
            panic_switch: None,
            broken: false,
        }
    }

    pub fn with_window(mut self, window: WindowMode) -> Self {
        self.window = window;
        self
    }

    pub fn with_cohort(mut self, cohort: CohortMode) -> Self {
        self.cohort = cohort;
        self
    }

    pub fn per_impact(mut self) -> Self {
        self.emission = Emission::PerImpact;
        self
    }

    pub fn with_dims(mut self, dims: Dims) -> Self {
        self.dims = dims;
        self
    }

    pub fn also_interested_in(mut self, noun: NounName) -> Self {
        self.also = Some(noun);
        self
    }

    pub fn gated(mut self, gate: Arc<Gate>) -> Self {
        self.gate = Some(gate);
        self
    }

    pub fn gated_load(mut self, gate: Arc<Gate>) -> Self {
        self.load_gate = Some(gate);
        self
    }

    pub fn broken(mut self) -> Self {
        self.broken = true;
        self
    }

    pub fn with_fail_switch(mut self, switch: Arc<AtomicBool>) -> Self {
        self.fail_switch = Some(switch);
        self
    }

    pub fn with_panic_switch(mut self, switch: Arc<AtomicBool>) -> Self {
        self.panic_switch = Some(switch);
        self
    }

    pub(super) fn switched_off(&self) -> Result<(), EngineError> {
        if self
            .fail_switch
            .as_ref()
            .is_some_and(|switch| switch.load(Ordering::Relaxed))
        {
            return Err(EngineError::Service(
                "the sample slice was switched to fail for the duration of the test".into(),
            ));
        }
        Ok(())
    }
}
