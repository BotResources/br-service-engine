use std::time::Duration;

use sqlx::{PgConnection, PgPool};

use crate::error::{EngineError, RelayError};
use crate::housekeeping::leader::{self, Lease, SlotName};
use crate::name::{PodId, RelayName};

fn engine(error: EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}

pub(crate) struct OfferLeader {
    pod: PodId,
    lease: Duration,
}

impl OfferLeader {
    pub(crate) fn new(pod: PodId, lease: Duration) -> Self {
        Self { pod, lease }
    }

    pub(crate) async fn claim(
        &self,
        pg: &PgPool,
        name: &RelayName,
    ) -> Result<Option<Lease>, RelayError> {
        let slot_name = SlotName::Relay(name.clone());
        let key = leader::advisory_key(&slot_name);
        let mut tx = pg.begin().await?;
        if !leader::try_advisory_xact_lock(&mut tx, key)
            .await
            .map_err(engine)?
        {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        let lease = leader::claim_singleton_lease(&mut tx, slot_name, &self.pod, self.lease)
            .await
            .map_err(engine)?;
        match lease {
            Some(lease) => {
                tx.commit().await?;
                Ok(Some(lease))
            }
            None => {
                let _ = tx.rollback().await;
                Ok(None)
            }
        }
    }

    pub(crate) async fn still_leader(
        &self,
        conn: &mut PgConnection,
        lease: &mut Lease,
    ) -> Result<bool, RelayError> {
        leader::renew_slot(conn, lease, self.lease)
            .await
            .map_err(engine)
    }
}
