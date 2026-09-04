use std::sync::atomic::{AtomicU64, Ordering};

use crate::render::pass::PassReport;
use crate::render::repair::RepairCost;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RenderMetrics {
    pub passes: u64,
    pub impacts: u64,
    pub deltas: u64,
    pub discarded: u64,
    pub resets: u64,
    pub lag_resets: u64,
    pub overflows: u64,
    pub faults: u64,
    pub repairs: u64,
    pub populates: u64,
    pub loads: u64,
    pub projections: u64,
    pub sessions_ended: u64,
    pub transport_reconnects: u64,
    pub transport_incidents: u64,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) passes: AtomicU64,
    pub(crate) impacts: AtomicU64,
    pub(crate) deltas: AtomicU64,
    pub(crate) discarded: AtomicU64,
    pub(crate) resets: AtomicU64,
    pub(crate) lag_resets: AtomicU64,
    pub(crate) overflows: AtomicU64,
    pub(crate) faults: AtomicU64,
    pub(crate) repairs: AtomicU64,
    pub(crate) populates: AtomicU64,
    pub(crate) loads: AtomicU64,
    pub(crate) projections: AtomicU64,
    pub(crate) sessions_ended: AtomicU64,
    pub(crate) transport_reconnects: AtomicU64,
    pub(crate) transport_incidents: AtomicU64,
}

impl Counters {
    pub(crate) fn absorb(&self, report: &PassReport) {
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.impacts
            .fetch_add(report.impacts as u64, Ordering::Relaxed);
        self.deltas
            .fetch_add(report.deltas as u64, Ordering::Relaxed);
        self.discarded
            .fetch_add(report.discarded as u64, Ordering::Relaxed);
        self.faults
            .fetch_add(report.faults.len() as u64, Ordering::Relaxed);
        self.repairs
            .fetch_add(report.repaired() as u64, Ordering::Relaxed);
        self.resets
            .fetch_add(report.resets as u64, Ordering::Relaxed);
        self.lag_resets
            .fetch_add(report.lagged as u64, Ordering::Relaxed);
        self.populates
            .fetch_add(report.populates as u64, Ordering::Relaxed);
        self.loads.fetch_add(report.loads as u64, Ordering::Relaxed);
        self.projections
            .fetch_add(report.projections as u64, Ordering::Relaxed);
        self.sessions_ended
            .fetch_add(report.ended as u64, Ordering::Relaxed);
    }

    pub(crate) fn absorb_repair(&self, cost: &RepairCost) {
        self.populates
            .fetch_add(cost.populates as u64, Ordering::Relaxed);
        self.loads.fetch_add(cost.loads as u64, Ordering::Relaxed);
        self.projections
            .fetch_add(cost.projections as u64, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RenderMetrics {
        RenderMetrics {
            passes: self.passes.load(Ordering::Relaxed),
            impacts: self.impacts.load(Ordering::Relaxed),
            deltas: self.deltas.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            resets: self.resets.load(Ordering::Relaxed),
            lag_resets: self.lag_resets.load(Ordering::Relaxed),
            overflows: self.overflows.load(Ordering::Relaxed),
            faults: self.faults.load(Ordering::Relaxed),
            repairs: self.repairs.load(Ordering::Relaxed),
            populates: self.populates.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
            projections: self.projections.load(Ordering::Relaxed),
            sessions_ended: self.sessions_ended.load(Ordering::Relaxed),
            transport_reconnects: self.transport_reconnects.load(Ordering::Relaxed),
            transport_incidents: self.transport_incidents.load(Ordering::Relaxed),
        }
    }
}
