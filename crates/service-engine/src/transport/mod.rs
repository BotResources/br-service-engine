mod listening;
pub mod payload;
pub mod pg;
pub mod probe;
pub mod reassemble;

use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::PgConnection;

pub use payload::Frame;
pub use pg::PgListenNotify;
pub use probe::ListenerProbe;
pub use reassemble::Reassembler;

use crate::error::{EngineError, TransportError};
use crate::impact::{Impact, TransportEvent};
use crate::name::NounName;
use crate::time::Timestamp;
use crate::wire::KeyBytes;

pub const NOTIFY_PAYLOAD_LIMIT: usize = 8000;

pub trait ImpactTransport: Send + Sync {
    fn stage_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    fn schedule_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        noun: NounName,
        key: KeyBytes,
        at: Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>>;
}

#[derive(Debug, Default)]
pub struct PendingImpacts(Vec<Impact>);

impl PendingImpacts {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, impact: Impact) {
        self.0.push(impact);
    }

    pub fn extend(&mut self, impacts: impl IntoIterator<Item = Impact>) {
        self.0.extend(impacts);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub async fn stage_in(
        self,
        transport: &dyn ImpactTransport,
        conn: &mut PgConnection,
    ) -> Result<(), EngineError> {
        if self.0.is_empty() {
            return Ok(());
        }
        transport.stage_in(conn, &self.0).await
    }
}

impl FromIterator<Impact> for PendingImpacts {
    fn from_iter<I: IntoIterator<Item = Impact>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
