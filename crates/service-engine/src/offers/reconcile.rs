use std::collections::HashSet;

use sqlx::PgPool;

use crate::error::RelayError;
use crate::housekeeping::leader::Lease;
use crate::mirror::{OfferManifest, manifest_key};
use crate::name::RelayName;
use crate::nats::{KvBucket, KvKey, KvPrefix, Slot};
use crate::offer::Offer;
use crate::offers::fault;
use crate::offers::leader::OfferLeader;
use crate::offers::marker;
use crate::persistence::Aggregate;
use crate::wire::KeyBytes;

pub(crate) async fn reconcile<O: Offer>(
    pg: &PgPool,
    name: &RelayName,
    bucket: &KvBucket<O::Published>,
    manifest: &KvBucket<OfferManifest>,
    leader: &OfferLeader,
    lease: &mut Lease,
    chunk: usize,
) -> Result<(), RelayError> {
    publish_manifest::<O>(pg, name, manifest).await?;
    let mut conn = pg.acquire().await?;
    let rows = O::all(&mut conn).await.map_err(relay)?;
    drop(conn);

    let mut live: HashSet<KvKey> = HashSet::with_capacity(rows.len());
    let mut puts: Vec<(KvKey, KeyBytes)> = Vec::with_capacity(rows.len());
    for row in &rows {
        let kv_key = O::key(row).map_err(relay)?;
        let agg_key = KeyBytes::encode(&row.key()).map_err(relay)?;
        live.insert(kv_key.clone());
        puts.push((kv_key, agg_key));
    }

    let prefix = KvPrefix::new(O::PREFIX).map_err(|error| {
        RelayError::Publish(format!(
            "offer prefix {}: {}",
            O::PREFIX,
            crate::chain::describe(&error)
        ))
    })?;
    let mut retracts: Vec<KvKey> = Vec::new();
    for (kv_key, slot, _) in bucket
        .entries_isolating(&prefix)
        .await
        .map_err(RelayError::published_language)?
    {
        fault::isolate(pg, name, &kv_key, &slot, "the value").await?;
        if !live.contains(&kv_key) {
            retracts.push(kv_key);
        }
    }

    let chunk = chunk.max(1);
    for group in puts.chunks(chunk) {
        let mut tx = pg.begin().await?;
        if !leader.still_leader(&mut tx, lease).await? {
            let _ = tx.rollback().await;
            return Ok(());
        }
        for (kv_key, agg_key) in group {
            marker::stage(&mut tx, name, kv_key, Some(agg_key)).await?;
        }
        tx.commit().await?;
    }
    for group in retracts.chunks(chunk) {
        let mut tx = pg.begin().await?;
        if !leader.still_leader(&mut tx, lease).await? {
            let _ = tx.rollback().await;
            return Ok(());
        }
        for kv_key in group {
            marker::stage(&mut tx, name, kv_key, None).await?;
        }
        tx.commit().await?;
    }
    Ok(())
}

async fn publish_manifest<O: Offer>(
    pg: &PgPool,
    name: &RelayName,
    manifest: &KvBucket<OfferManifest>,
) -> Result<(), RelayError> {
    let key = manifest_key(O::PREFIX).map_err(relay)?;
    let want = OfferManifest {
        prefix: O::PREFIX.to_string(),
        version: O::VERSION,
    };
    let held = manifest
        .get_isolating(&key)
        .await
        .map_err(RelayError::published_language)?;
    if let Some((slot, _)) = &held {
        fault::isolate(pg, name, &key, slot, "the offer manifest").await?;
    }
    match held {
        Some((Slot::Value(found), _)) if found == want => Ok(()),
        Some((_, revision)) => manifest
            .update_if(&key, &want, revision)
            .await
            .map(|_| ())
            .map_err(RelayError::published_language),
        None => manifest
            .put(&key, &want)
            .await
            .map_err(RelayError::published_language),
    }
}

fn relay(error: crate::error::EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}
