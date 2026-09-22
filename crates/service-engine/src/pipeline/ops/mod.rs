use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgConnection;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::{BlobHandle, BlobRef};
use crate::error::EngineError;
use crate::gate::Reason;
use crate::offers::OfferStagers;
use crate::pipeline::outbound::OutboundContext;
use crate::pipeline::policy::Policies;
use crate::pipeline::staged::Staged;
use crate::time::Timestamp;

mod emit;
mod locking;
mod persist;
mod policy;

const KEY_REUSED: Reason = Reason::new("KEY_REUSED");

pub struct Ops<'a> {
    pub(crate) conn: &'a mut PgConnection,
    pub(crate) staged: &'a mut Staged,
    pub(crate) accumulators: &'a AccumulatorRuntime,
    pub(crate) offers: Arc<OfferStagers>,
    pub(crate) policies: Arc<Policies>,
    pub(crate) blobs: Option<&'a BlobHandle>,
    pub(crate) now: Timestamp,
    outbound: Option<OutboundContext>,
    blob_seen: HashMap<(TypeId, Vec<u8>), Vec<BlobRef>>,
    loaded: HashMap<(TypeId, Vec<u8>), Box<dyn Any + Send + Sync>>,
}

impl<'a> Ops<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        staged: &'a mut Staged,
        accumulators: &'a AccumulatorRuntime,
        offers: Arc<OfferStagers>,
        policies: Arc<Policies>,
        blobs: Option<&'a BlobHandle>,
        now: Timestamp,
    ) -> Self {
        Self {
            conn,
            staged,
            accumulators,
            offers,
            policies,
            blobs,
            now,
            outbound: None,
            blob_seen: HashMap::new(),
            loaded: HashMap::new(),
        }
    }

    pub(crate) fn with_outbound(mut self, outbound: OutboundContext) -> Self {
        self.outbound = Some(outbound);
        self
    }

    pub(super) fn outbound(&self) -> Result<&OutboundContext, EngineError> {
        self.outbound.as_ref().ok_or_else(|| {
            EngineError::Config(
                "this pipeline has no outbound identity, so it cannot emit an integration event \
                 or command onto the frontier"
                    .into(),
            )
        })
    }

    pub fn now(&self) -> Timestamp {
        self.now
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.conn
    }
}
