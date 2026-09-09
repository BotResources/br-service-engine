use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgConnection;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::engine::BlobReader;
use crate::erase::context::Erase;
use crate::erase::event::PersonErased;
use crate::erase::manifest::Erased;
use crate::erase::person::PersonId;
use crate::erase::purge;
use crate::erase::registry::ErasedErasable;
use crate::error::EngineError;
use crate::offers::OfferStagers;
use crate::pipeline::{Ops, Staged, begin_scoped, event_record, flush_and_commit};
use crate::presence::PresenceHandle;
use crate::principal::Principal;
use crate::schema::TABLE_PERSON_ERASURE;
use crate::time::{self, Timestamp};
use crate::transport::ImpactTransport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EraseOutcome {
    pub fresh: bool,
    pub rows_erased: u64,
    pub blobs_purged: u64,
}

pub(crate) trait ErasureDrain: Send + Sync + 'static {
    fn drain(&self) -> BoxFuture<'_, Result<u64, EngineError>>;
}

pub struct Eraser<P: Principal> {
    pg: PgPool,
    transport: Arc<dyn ImpactTransport>,
    accumulators: Arc<AccumulatorRuntime>,
    offers: Arc<OfferStagers>,
    presence: PresenceHandle<P>,
    blobs: BlobReader,
    erasables: Arc<Vec<Arc<dyn ErasedErasable>>>,
    service: Option<String>,
    lock_timeout: Duration,
}

impl<P: Principal> Eraser<P> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pg: PgPool,
        transport: Arc<dyn ImpactTransport>,
        accumulators: Arc<AccumulatorRuntime>,
        offers: Arc<OfferStagers>,
        presence: PresenceHandle<P>,
        blobs: BlobReader,
        erasables: Arc<Vec<Arc<dyn ErasedErasable>>>,
        service: Option<String>,
        lock_timeout: Duration,
    ) -> Self {
        Self {
            pg,
            transport,
            accumulators,
            offers,
            presence,
            blobs,
            erasables,
            service,
            lock_timeout,
        }
    }

    pub async fn erase(&self, person: PersonId) -> Result<EraseOutcome, EngineError> {
        let now = time::now();
        let mut tx = begin_scoped(&self.pg, self.lock_timeout)
            .await
            .map_err(EngineError::from)?;
        set_erase_context(&mut tx).await?;
        let fresh = record_erasure(&mut tx, person, now).await?;
        let mut staged = Staged::default();
        let mut manifest = Erased::new();
        for erasable in self.erasables.iter() {
            let ops = Ops::new(
                &mut tx,
                &mut staged,
                self.accumulators.as_ref(),
                self.offers.clone(),
                None,
                now,
            );
            let mut cx = Erase::new(ops, person);
            let touched = run_slice(erasable.as_ref(), &mut cx, person).await?;
            manifest.merge(touched);
        }
        if fresh {
            persist_manifest(&mut tx, person, &manifest).await?;
            let service = self.service.as_deref().ok_or_else(|| {
                EngineError::Config(
                    "erase needs a configured service to name the PersonErased producer; call \
                     EngineConfig::with_service"
                        .into(),
                )
            })?;
            let outbound = crate::pipeline::OutboundContext {
                actor: crate::identity::service_actor(service),
                correlation_id: uuid::Uuid::now_v7(),
                causation_id: None,
                producer: Some(service.to_string()),
            };
            staged
                .outbox
                .push(event_record(&PersonErased::new(service, person)?, &outbound)?);
        }
        flush_and_commit(tx, &staged, self.transport.as_ref()).await?;

        let blobs_purged =
            purge::complete_one(&self.pg, &self.presence, &self.blobs, person).await?;
        Ok(EraseOutcome {
            fresh,
            rows_erased: manifest.row_count(),
            blobs_purged,
        })
    }
}

impl<P: Principal> ErasureDrain for Eraser<P> {
    fn drain(&self) -> BoxFuture<'_, Result<u64, EngineError>> {
        Box::pin(async move { purge::drain_pending(&self.pg, &self.presence, &self.blobs).await })
    }
}

async fn run_slice<'a>(
    erasable: &'a dyn ErasedErasable,
    cx: &'a mut Erase<'a>,
    person: PersonId,
) -> Result<Erased, EngineError> {
    erasable.erase(cx, person).await
}

async fn set_erase_context(conn: &mut PgConnection) -> Result<(), EngineError> {
    sqlx::query("SELECT set_config('app.erasing', 'on', true)")
        .execute(conn)
        .await?;
    Ok(())
}

async fn record_erasure(
    conn: &mut PgConnection,
    person: PersonId,
    at: Timestamp,
) -> Result<bool, EngineError> {
    let inserted = sqlx::query(&format!(
        "INSERT INTO {TABLE_PERSON_ERASURE} (person_id, erased_at) VALUES ($1, $2) \
         ON CONFLICT (person_id) DO NOTHING"
    ))
    .bind(person.as_uuid())
    .bind(at)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(inserted == 1)
}

async fn persist_manifest(
    conn: &mut PgConnection,
    person: PersonId,
    manifest: &Erased,
) -> Result<(), EngineError> {
    let value = manifest.to_purge()?.to_value()?;
    sqlx::query(&format!(
        "UPDATE {TABLE_PERSON_ERASURE} SET manifest = $2 WHERE person_id = $1"
    ))
    .bind(person.as_uuid())
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}
