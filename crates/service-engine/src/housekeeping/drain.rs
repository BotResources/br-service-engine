use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::error::{EngineError, RelayError};
use crate::housekeeping::leader::{self, SlotName};
use crate::name::PodId;
use crate::relay::{Claim, Discipline, Drained, Relay};

pub(crate) async fn drain_one(
    pg: &PgPool,
    relay: Arc<dyn Relay>,
    discipline: Discipline,
    pod: &PodId,
    batch: usize,
    lease: Duration,
    slot_period: Duration,
) -> Result<Option<Drained>, RelayError> {
    let claim = Claim::new(pod.clone(), batch);
    if let Some(hosted) = relay.hosted_drain(pg, &claim) {
        if discipline == Discipline::Leader {
            return Err(engine(EngineError::Service(
                format!(
                    "{} drains through a hosted relay, which the Leader discipline cannot serve: \
                     the slot claim and its completion must ride the drain's own transaction",
                    relay.name()
                )
                .into(),
            )));
        }
        return hosted.await.map(Some);
    }
    let mut tx = pg.begin().await?;
    if discipline == Discipline::Leader {
        let name = relay.name();
        let slot_name = SlotName::Relay(name.clone());
        let key = leader::advisory_key(&slot_name);
        if !leader::try_advisory_xact_lock(&mut tx, key)
            .await
            .map_err(engine)?
        {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        let Some(held) = leader::claim_current_slot(&mut tx, slot_name, slot_period, pod, lease)
            .await
            .map_err(engine)?
        else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let drained = match relay.drain(&mut tx, &claim).await {
            Ok(drained) => drained,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        };
        if !leader::complete_slot(&mut tx, &held)
            .await
            .map_err(engine)?
        {
            let _ = tx.rollback().await;
            return Err(engine(EngineError::Service(
                format!(
                    "the slot {} of {name} was taken away before its drain completed",
                    held.slot()
                )
                .into(),
            )));
        }
        tx.commit().await?;
        return Ok(Some(drained));
    }
    let drained = match relay.drain(&mut tx, &claim).await {
        Ok(drained) => drained,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(error);
        }
    };
    tx.commit().await?;
    Ok(Some(drained))
}

pub(crate) fn engine(error: EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::describe;

    #[test]
    fn a_failure_reason_names_the_whole_cause_chain_not_only_its_top_word() {
        let error = RelayError::Relay(Box::new(EngineError::Service(
            "permission denied for table sample_relay_row".into(),
        )));
        assert_eq!(
            describe(&error),
            "relay: service: permission denied for table sample_relay_row"
        );
        assert_eq!(
            describe(&RelayError::Publish("no such stream".to_string())),
            "publishing a claimed row: no such stream"
        );
    }
}
