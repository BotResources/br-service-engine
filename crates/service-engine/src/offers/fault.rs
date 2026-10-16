use sqlx::PgPool;

use crate::error::RelayError;
use crate::inbound::stage_offer_value;
use crate::name::RelayName;
use crate::nats::{KvKey, Slot};

pub(crate) async fn isolate<V>(
    pg: &PgPool,
    offer: &RelayName,
    key: &KvKey,
    slot: &Slot<V>,
    what: &str,
) -> Result<(), RelayError> {
    let Slot::Undecodable(fault) = slot else {
        return Ok(());
    };
    let reason = fault.reason_for(what);
    tracing::warn!(
        offer = %offer,
        key = key.as_str(),
        reason = %reason,
        "an undecodable value sits under the offer prefix; it is dead-lettered and overwritten or retracted",
    );
    let mut tx = pg.begin().await?;
    stage_offer_value(&mut tx, offer.as_str(), key.as_str(), &reason).await?;
    tx.commit().await?;
    Ok(())
}
