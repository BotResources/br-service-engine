use sqlx::PgConnection;
use uuid::Uuid;

use crate::blobs::{BlobRowOp, insert_reference, orphan_reference};
use crate::error::EngineError;
use crate::gate::Reason;
use crate::impact::Impact;
use crate::inbound::Source;
use crate::name::{AccumulatorName, NounName};
use crate::offers::OfferDirty;
use crate::relays::outbox::{OutboxRecord, stage as stage_outbox};
use crate::schema::{TABLE_OFFER_DIRTY, TABLE_SCHEDULED_MESSAGE};
use crate::time::Timestamp;
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RefusalOrigin {
    CreatePrecondition,
    PostSavePolicy,
}

impl RefusalOrigin {
    pub(crate) fn detail(self) -> &'static str {
        match self {
            Self::CreatePrecondition => "a create precondition refused the write",
            Self::PostSavePolicy => "a post-save policy refused the write",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StagedRefusal {
    pub reason: Reason,
    pub origin: RefusalOrigin,
}

pub(crate) struct ScheduledMessage {
    pub at: Timestamp,
    pub source: Source,
    pub subject: String,
    pub message_id: Uuid,
    pub payload: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct Staged {
    pub impacts: Vec<Impact>,
    pub scheduled_impacts: Vec<(NounName, KeyBytes, Timestamp)>,
    pub outbox: Vec<OutboxRecord>,
    pub scheduled_messages: Vec<ScheduledMessage>,
    pub terminal_violation: Option<String>,
    pub policy_refusal: Option<StagedRefusal>,
    pub offer_dirty: Vec<OfferDirty>,
    pub blob_ops: Vec<BlobRowOp>,
    pub sealed_keys: Vec<(AccumulatorName, KeyBytes)>,
}

impl Staged {
    pub(crate) fn is_within(&self, impacts_per_commit: usize) -> bool {
        self.impacts.len() <= impacts_per_commit
    }

    pub(crate) async fn flush(
        &self,
        conn: &mut PgConnection,
        transport: &dyn ImpactTransport,
    ) -> Result<(), EngineError> {
        if !self.impacts.is_empty() {
            transport.stage_in(conn, &self.impacts).await?;
        }
        for (noun, key, at) in &self.scheduled_impacts {
            transport
                .schedule_in(conn, noun.clone(), key.clone(), *at)
                .await?;
        }
        for record in &self.outbox {
            stage_outbox(&mut *conn, record).await?;
        }
        for message in &self.scheduled_messages {
            insert_scheduled_message(conn, message).await?;
        }
        for dirty in &self.offer_dirty {
            stage_offer_dirty(conn, dirty).await?;
        }
        for op in &self.blob_ops {
            match op {
                BlobRowOp::Insert(row) => insert_reference(conn, row).await?,
                BlobRowOp::Orphan(reference, at) => {
                    orphan_reference(conn, *reference, *at).await?;
                }
            }
        }
        Ok(())
    }
}

async fn stage_offer_dirty(conn: &mut PgConnection, dirty: &OfferDirty) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "INSERT INTO {TABLE_OFFER_DIRTY} (offer, kv_key, agg_key, seq) \
           VALUES ($1, $2, $3, nextval('service_engine.offer_dirty_seq')) \
         ON CONFLICT (offer, kv_key) DO UPDATE \
           SET agg_key = EXCLUDED.agg_key, seq = nextval('service_engine.offer_dirty_seq')"
    ))
    .bind(dirty.offer)
    .bind(dirty.kv_key.as_str())
    .bind(dirty.agg_key.as_slice().to_vec())
    .execute(conn)
    .await?;
    Ok(())
}

async fn insert_scheduled_message(
    conn: &mut PgConnection,
    message: &ScheduledMessage,
) -> Result<(), EngineError> {
    let source = match message.source {
        Source::Command => "command",
        Source::Event => "event",
    };
    sqlx::query(&format!(
        "INSERT INTO {TABLE_SCHEDULED_MESSAGE} \
           (id, at, source, subject, message_id, payload) \
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(Uuid::now_v7())
    .bind(message.at.as_datetime())
    .bind(source)
    .bind(&message.subject)
    .bind(message.message_id)
    .bind(&message.payload)
    .execute(conn)
    .await?;
    Ok(())
}
