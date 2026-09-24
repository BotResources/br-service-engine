use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::error::RelayError;
use crate::nats::{KvBucket, KvKey, NatsError, Revision};
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

pub(crate) async fn apply_cas<V>(
    bucket: &KvBucket<V>,
    key: &KvKey,
    write: &Write<V>,
    observed: Option<(V, Revision)>,
) -> Result<KvOutcome, RelayError>
where
    V: Serialize + DeserializeOwned + PartialEq,
{
    match (write, observed) {
        (Write::Put(value), None) => match bucket.create(key, value).await {
            Ok(_) => Ok(KvOutcome::Applied),
            Err(NatsError::RevisionConflict { .. }) => Ok(KvOutcome::Conflict),
            Err(error) => Err(RelayError::published_language(error)),
        },
        (Write::Put(value), Some((current, revision))) => {
            if current == *value {
                return Ok(KvOutcome::Applied);
            }
            match bucket.update_if(key, value, revision).await {
                Ok(_) => Ok(KvOutcome::Applied),
                Err(NatsError::RevisionConflict { .. }) => Ok(KvOutcome::Conflict),
                Err(error) => Err(RelayError::published_language(error)),
            }
        }
        (Write::Retract, None) => Ok(KvOutcome::Applied),
        (Write::Retract, Some((_, revision))) => match bucket.delete_if(key, revision).await {
            Ok(()) => Ok(KvOutcome::Applied),
            Err(NatsError::RevisionConflict { .. }) => Ok(KvOutcome::Conflict),
            Err(error) => Err(RelayError::published_language(error)),
        },
    }
}

fn relay(error: crate::error::EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}
