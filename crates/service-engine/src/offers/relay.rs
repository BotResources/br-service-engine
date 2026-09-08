use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use sqlx::{PgConnection, Row};
use tokio::sync::OnceCell;

use crate::error::RelayError;
use crate::name::RelayName;
use crate::nats::{KvBucket, KvKey, KvPrefix, Nats, NatsError};
use crate::offer::Offer;
use crate::persistence::{Aggregate, Persistence};
use crate::relay::{Claim, Discipline, Drained, Relay};
use crate::relays::kv::DEFAULT_CAS_RETRIES;
use crate::relays::kv_watermark;
use crate::schema::TABLE_OFFER_DIRTY;
use crate::wire::KeyBytes;

enum Write<V> {
    Put(V),
    Retract,
}

struct Marker {
    kv_key: KvKey,
    agg_key: Option<KeyBytes>,
    seq: u64,
}

pub(crate) struct OfferRelay<O: Offer> {
    name: RelayName,
    nats: Nats,
    bucket: OnceCell<KvBucket<O::Published>>,
    reconciled: AtomicBool,
    _offer: PhantomData<fn() -> O>,
}

impl<O: Offer> OfferRelay<O> {
    pub(crate) fn new(nats: Nats) -> Result<Self, RelayError> {
        Ok(Self {
            name: RelayName::new(O::NAME)
                .map_err(|error| RelayError::Publish(format!("offer name {}: {error}", O::NAME)))?,
            nats,
            bucket: OnceCell::new(),
            reconciled: AtomicBool::new(false),
            _offer: PhantomData,
        })
    }

    async fn bucket(&self) -> Result<&KvBucket<O::Published>, RelayError> {
        self.bucket
            .get_or_try_init(|| async {
                self.nats
                    .published_language::<O::Published>()
                    .await
                    .map_err(published_language)
            })
            .await
    }

    async fn reconcile(&self, conn: &mut PgConnection) -> Result<(), RelayError> {
        let bucket = self.bucket().await?;
        let rows = O::all(conn).await.map_err(relay)?;
        let mut live = Vec::new();
        for row in &rows {
            let kv_key = O::key(row).map_err(relay)?;
            let agg_key = KeyBytes::encode(&row.key()).map_err(relay)?;
            live.push(kv_key.clone());
            stage_marker(conn, &self.name, &kv_key, Some(&agg_key)).await?;
        }
        let prefix = KvPrefix::new(O::PREFIX)
            .map_err(|error| RelayError::Publish(format!("offer prefix {}: {error}", O::PREFIX)))?;
        for (kv_key, _) in bucket.entries(&prefix).await.map_err(published_language)? {
            if !live.contains(&kv_key) {
                stage_marker(conn, &self.name, &kv_key, None).await?;
            }
        }
        Ok(())
    }

    async fn pending(
        &self,
        conn: &mut PgConnection,
        batch: usize,
    ) -> Result<Vec<Marker>, RelayError> {
        let rows = sqlx::query(&format!(
            "SELECT kv_key, agg_key, seq FROM {TABLE_OFFER_DIRTY} \
             WHERE offer = $1 ORDER BY seq LIMIT $2 {}",
            Claim::FOR_UPDATE_SKIP_LOCKED
        ))
        .bind(self.name.as_str())
        .bind(i64::try_from(batch).unwrap_or(i64::MAX))
        .fetch_all(&mut *conn)
        .await?;
        rows.into_iter()
            .map(|row| {
                let kv_key = KvKey::new(row.get::<String, _>("kv_key"))
                    .map_err(|error| RelayError::Publish(format!("offer kv key: {error}")))?;
                let agg_key = row
                    .get::<Option<Vec<u8>>, _>("agg_key")
                    .map(|bytes| KeyBytes::from_json_bytes(Bytes::from(bytes)))
                    .transpose()
                    .map_err(relay)?;
                let seq = u64::try_from(row.get::<i64, _>("seq")).unwrap_or_default();
                Ok(Marker {
                    kv_key,
                    agg_key,
                    seq,
                })
            })
            .collect()
    }

    async fn resolve(
        &self,
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

    async fn apply(
        &self,
        conn: &mut PgConnection,
        marker: &Marker,
        write: Write<O::Published>,
    ) -> Result<(), RelayError> {
        let bucket = self.bucket().await?;
        let watermark = kv_watermark::read(conn, &self.name, &marker.kv_key).await?;
        if watermark.is_some_and(|held| marker.seq <= held) {
            return delete_marker(conn, &self.name, marker).await;
        }
        for _ in 0..=DEFAULT_CAS_RETRIES {
            let observed = bucket
                .get_with_revision(&marker.kv_key)
                .await
                .map_err(published_language)?;
            match (&write, observed) {
                (Write::Put(value), None) => {
                    bucket
                        .put(&marker.kv_key, value)
                        .await
                        .map_err(published_language)?;
                }
                (Write::Put(value), Some((_, revision))) => {
                    match bucket.update_if(&marker.kv_key, value, revision).await {
                        Ok(_) => {}
                        Err(NatsError::RevisionConflict { .. }) => continue,
                        Err(error) => return Err(published_language(error)),
                    }
                }
                (Write::Retract, None) => {}
                (Write::Retract, Some((_, revision))) => {
                    match bucket.delete_if(&marker.kv_key, revision).await {
                        Ok(()) => {}
                        Err(NatsError::RevisionConflict { .. }) => continue,
                        Err(error) => return Err(published_language(error)),
                    }
                }
            }
            kv_watermark::raise(conn, &self.name, &marker.kv_key, marker.seq).await?;
            return delete_marker(conn, &self.name, marker).await;
        }
        Err(RelayError::Publish(format!(
            "offer {} lost the published-language compare-and-swap on {} after {DEFAULT_CAS_RETRIES} retries",
            self.name,
            marker.kv_key.as_str()
        )))
    }
}

impl<O: Offer> Relay for OfferRelay<O> {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::Leader
    }

    fn drain<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            let first = self
                .reconciled
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
            if first {
                self.reconcile(conn).await?;
            }
            let batch = claim.batch().max(1);
            let markers = self.pending(conn, batch).await?;
            if markers.is_empty() {
                return Ok(Drained::NOTHING);
            }
            let rows = markers.len();
            for marker in &markers {
                let write = self.resolve(conn, marker).await?;
                self.apply(conn, marker, write).await?;
            }
            Ok(Drained::rows(rows, first || rows >= batch))
        })
    }
}

async fn stage_marker(
    conn: &mut PgConnection,
    name: &RelayName,
    kv_key: &KvKey,
    agg_key: Option<&KeyBytes>,
) -> Result<(), RelayError> {
    sqlx::query(&format!(
        "INSERT INTO {TABLE_OFFER_DIRTY} (offer, kv_key, agg_key, seq) \
         VALUES ($1, $2, $3, nextval('service_engine.offer_dirty_seq')) \
         ON CONFLICT (offer, kv_key) DO UPDATE \
           SET agg_key = EXCLUDED.agg_key, seq = nextval('service_engine.offer_dirty_seq')"
    ))
    .bind(name.as_str())
    .bind(kv_key.as_str())
    .bind(agg_key.map(|key| key.as_slice().to_vec()))
    .execute(conn)
    .await?;
    Ok(())
}

async fn delete_marker(
    conn: &mut PgConnection,
    name: &RelayName,
    marker: &Marker,
) -> Result<(), RelayError> {
    sqlx::query(&format!(
        "DELETE FROM {TABLE_OFFER_DIRTY} WHERE offer = $1 AND kv_key = $2 AND seq = $3"
    ))
    .bind(name.as_str())
    .bind(marker.kv_key.as_str())
    .bind(i64::try_from(marker.seq).unwrap_or(i64::MAX))
    .execute(conn)
    .await?;
    Ok(())
}

fn published_language(error: NatsError) -> RelayError {
    RelayError::Publish(format!("published language: {error}"))
}

fn relay(error: crate::error::EngineError) -> RelayError {
    RelayError::Relay(Box::new(error))
}
