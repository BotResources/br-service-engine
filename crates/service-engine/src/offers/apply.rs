use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::error::RelayError;
use crate::nats::{KvBucket, KvKey, NatsError};
use crate::offer::Offer;
use crate::offers::marker::Marker;
use crate::persistence::{Aggregate, Persistence};

pub(crate) enum Write<V> {
    Put(V),
    Retract,
}

pub(crate) enum KvOutcome {
    Applied,
    Conflict,
}

pub(crate) async fn resolve<O: Offer>(
    conn: &mut PgConnection,
    marker: &Marker,
) -> Result<Write<O::Published>, RelayError> {
    let Some(agg_key) = &marker.agg_key else {
        return Ok(Write::Retract);
    };
    let key = agg_key
        .decode::<<<O::Row as Aggregate>::Store as Persistence>::Key>()
        .map_err(relay)?;
    match <O::Row as Aggregate>::Store::load(conn, &key)
        .await
        .map_err(relay)?
    {
        Some(row) => Ok(match O::publish(&row) {
            Some(value) => Write::Put(value),
            None => Write::Retract,
        }),
        None => Ok(Write::Retract),
    }
}

pub(crate) async fn apply_kv<V>(
    bucket: &KvBucket<V>,
    key: &KvKey,
    write: &Write<V>,
    retries: usize,
) -> Result<KvOutcome, RelayError>
where
    V: Serialize + DeserializeOwned + PartialEq,
{
    for _ in 0..=retries {
        let observed = bucket
            .get_with_revision(key)
            .await
            .map_err(published_language)?;
        match (write, observed) {
            (Write::Put(value), None) => match bucket.create(key, value).await {
                Ok(_) => return Ok(KvOutcome::Applied),
                Err(NatsError::RevisionConflict { .. }) => continue,
                Err(error) => return Err(published_language(error)),
            },
            (Write::Put(value), Some((observed, revision))) => {
                if observed == *value {
                    return Ok(KvOutcome::Applied);
                }
                match bucket.update_if(key, value, revision).await {
                    Ok(_) => return Ok(KvOutcome::Applied),
                    Err(NatsError::RevisionConflict { .. }) => continue,
                    Err(error) => return Err(published_language(error)),
                }
            }
            (Write::Retract, None) => return Ok(KvOutcome::Applied),
            (Write::Retract, Some((_, revision))) => match bucket.delete_if(key, revision).await {
                Ok(()) => return Ok(KvOutcome::Applied),
                Err(NatsError::RevisionConflict { .. }) => continue,
                Err(error) => return Err(published_language(error)),
            },
        }
    }
    Ok(KvOutcome::Conflict)
}

fn published_language(error: NatsError) -> RelayError {
    RelayError::Publish(format!("published language: {error}"))
}

fn relay(error: crate::error::EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}
