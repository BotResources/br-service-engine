use std::sync::Arc;
use std::time::Duration;

use sqlx::PgConnection;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::engine::BlobReader;
use crate::erase::context::Erase;
use crate::erase::event::PersonErased;
use crate::erase::manifest::Erased;
use crate::erase::person::PersonId;
use crate::erase::registry::ErasedErasable;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::offers::OfferStagers;
use crate::pipeline::{Ops, Staged, begin_scoped, event_record, flush_and_commit};
use crate::presence::PresenceHandle;
use crate::principal::Principal;
use crate::schema::{TABLE_ACCUMULATOR_CHUNK, TABLE_ACCUMULATOR_SEAL, TABLE_PERSON_ERASURE};
use crate::time::{self, Timestamp};
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EraseOutcome {
    pub fresh: bool,
    pub rows_erased: u64,
    pub blobs_purged: u64,
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
            let service = self.service.as_deref().ok_or_else(|| {
                EngineError::Config(
                    "erase needs a configured service to name the PersonErased producer; call \
                     EngineConfig::with_service"
                        .into(),
                )
            })?;
            staged
                .outbox
                .push(event_record(&PersonErased::new(service, person)?)?);
        }
        flush_and_commit(tx, &staged, self.transport.as_ref()).await?;

        self.purge_streams(manifest.streams()).await?;
        self.presence.purge(manifest.presence_keys()).await?;
        let named = self.blobs.purge_references(manifest.blob_refs()).await?;
        let owned = self.blobs.purge_person(person).await?;
        Ok(EraseOutcome {
            fresh,
            rows_erased: manifest.row_count(),
            blobs_purged: named + owned,
        })
    }

    async fn purge_streams(
        &self,
        streams: &[(AccumulatorName, KeyBytes)],
    ) -> Result<(), EngineError> {
        for (accumulator, key) in streams {
            let key_value = key.decode::<serde_json::Value>()?;
            sqlx::query(&format!(
                "DELETE FROM {TABLE_ACCUMULATOR_CHUNK} WHERE accumulator = $1 AND key = $2"
            ))
            .bind(accumulator.as_str())
            .bind(&key_value)
            .execute(&self.pg)
            .await?;
            sqlx::query(&format!(
                "DELETE FROM {TABLE_ACCUMULATOR_SEAL} WHERE accumulator = $1 AND key = $2"
            ))
            .bind(accumulator.as_str())
            .bind(&key_value)
            .execute(&self.pg)
            .await?;
        }
        Ok(())
    }
}

async fn run_slice<'a>(
    erasable: &'a dyn ErasedErasable,
    cx: &'a mut Erase<'a>,
    person: PersonId,
) -> Result<Erased, EngineError> {
    erasable.erase(cx, person).await
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
