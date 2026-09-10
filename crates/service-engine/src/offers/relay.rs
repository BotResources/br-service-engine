use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};
use tokio::sync::OnceCell;

use crate::error::RelayError;
use crate::housekeeping::leader::Lease;
use crate::name::RelayName;
use crate::nats::{KvBucket, Nats, NatsError, Revision};
use crate::offer::Offer;
use crate::offers::apply::{self, KvOutcome, Write};
use crate::offers::leader::OfferLeader;
use crate::offers::marker::{self, Marker};
use crate::offers::reconcile;
use crate::relay::{Claim, Discipline, Drained, Relay};
use crate::relays::kv_watermark;

type Observed<V> = Option<(V, Revision)>;

pub(crate) struct OfferRelay<O: Offer> {
    name: RelayName,
    nats: Nats,
    leader: OfferLeader,
    bucket: OnceCell<KvBucket<O::Published>>,
    reconcile_period: Duration,
    last_reconcile: Mutex<Option<Instant>>,
    _offer: PhantomData<fn() -> O>,
}

impl<O: Offer> OfferRelay<O> {
    pub(crate) fn new(
        nats: Nats,
        leader: OfferLeader,
        reconcile_period: Duration,
    ) -> Result<Self, RelayError> {
        Ok(Self {
            name: RelayName::new(O::NAME)
                .map_err(|error| RelayError::Publish(format!("offer name {}: {error}", O::NAME)))?,
            nats,
            leader,
            bucket: OnceCell::new(),
            reconcile_period,
            last_reconcile: Mutex::new(None),
            _offer: PhantomData,
        })
    }

    fn reconcile_due(&self) -> bool {
        let guard = self
            .last_reconcile
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *guard {
            None => true,
            Some(at) => at.elapsed() >= self.reconcile_period,
        }
    }

    fn mark_reconciled(&self) {
        *self
            .last_reconcile
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
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

    async fn hosted_run(&self, pg: &PgPool, batch: usize) -> Result<Drained, RelayError> {
        let batch = batch.max(1);
        let Some(mut lease) = self.leader.claim(pg, &self.name).await? else {
            return Ok(Drained::NOTHING);
        };
        let due = self.reconcile_due();
        if due {
            let bucket = self.bucket().await?;
            reconcile::reconcile::<O>(pg, &self.name, bucket, &self.leader, &mut lease, batch)
                .await?;
            self.mark_reconciled();
        }
        let drained = self.drain_batch(pg, &mut lease, batch).await?;
        Ok(Drained::rows(drained, due || drained >= batch))
    }

    async fn drain_batch(
        &self,
        pg: &PgPool,
        lease: &mut Lease,
        batch: usize,
    ) -> Result<usize, RelayError> {
        let resolved = self.claim_and_resolve(pg, lease, batch).await?;
        if resolved.is_empty() {
            self.finalize(pg, lease, Vec::new()).await?;
            return Ok(0);
        }
        #[cfg(feature = "test-support")]
        crate::offers::pause::wait().await;
        let total = resolved.len();
        let bucket = self.bucket().await?;
        let mut applied: Vec<Marker> = Vec::with_capacity(total);
        for (marker, watermark, write, observed) in resolved {
            if watermark.is_some_and(|held| marker.seq <= held) {
                applied.push(marker);
                continue;
            }
            match apply::apply_cas(bucket, &marker.kv_key, &write, observed).await? {
                KvOutcome::Applied => applied.push(marker),
                KvOutcome::Conflict => {}
            }
        }
        self.finalize(pg, lease, applied).await?;
        Ok(total)
    }

    #[allow(clippy::type_complexity)]
    async fn claim_and_resolve(
        &self,
        pg: &PgPool,
        lease: &mut Lease,
        batch: usize,
    ) -> Result<Vec<(Marker, Option<u64>, Write<O::Published>, Observed<O::Published>)>, RelayError>
    {
        let mut tx = pg.begin().await?;
        if !self.leader.still_leader(&mut tx, lease).await? {
            let _ = tx.rollback().await;
            return Ok(Vec::new());
        }
        let markers = marker::pending(&mut tx, &self.name, batch).await?;
        let mut resolved = Vec::with_capacity(markers.len());
        for marker in markers {
            let watermark = kv_watermark::read(&mut tx, &self.name, &marker.kv_key).await?;
            let write = apply::resolve::<O>(&mut tx, &marker).await?;
            resolved.push((marker, watermark, write));
        }
        tx.commit().await?;

        let bucket = self.bucket().await?;
        let mut out = Vec::with_capacity(resolved.len());
        for (marker, watermark, write) in resolved {
            let observed = bucket
                .get_with_revision(&marker.kv_key)
                .await
                .map_err(published_language)?;
            out.push((marker, watermark, write, observed));
        }
        Ok(out)
    }

    async fn finalize(
        &self,
        pg: &PgPool,
        lease: &mut Lease,
        applied: Vec<Marker>,
    ) -> Result<(), RelayError> {
        let mut tx = pg.begin().await?;
        if !self.leader.still_leader(&mut tx, lease).await? {
            let _ = tx.rollback().await;
            return Ok(());
        }
        for marker in &applied {
            kv_watermark::raise(&mut tx, &self.name, &marker.kv_key, marker.seq).await?;
            marker::delete(&mut tx, &self.name, marker).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

impl<O: Offer> Relay for OfferRelay<O> {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::Leader
    }

    fn self_fenced(&self) -> bool {
        true
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            Err(RelayError::Publish(format!(
                "offer {} drains through its own fenced transactions and is served by hosted_drain",
                self.name
            )))
        })
    }

    fn hosted_drain<'a>(
        &'a self,
        pg: &'a PgPool,
        claim: &'a Claim,
    ) -> Option<BoxFuture<'a, Result<Drained, RelayError>>> {
        Some(Box::pin(self.hosted_run(pg, claim.batch())))
    }
}

fn published_language(error: NatsError) -> RelayError {
    RelayError::Publish(format!("published language: {error}"))
}
