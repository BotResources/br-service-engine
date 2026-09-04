use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool, Row};
use tokio::sync::watch;
use uuid::Uuid;

use crate::error::{EngineError, TransportError};
use crate::impact::{Dims, Impact, TransportEvent};
use crate::name::{ChannelName, NounName};
use crate::schema::TABLE_SCHEDULED_IMPACT;
use crate::time::Timestamp;
use crate::transport::payload::encode;
use crate::transport::{ImpactTransport, NOTIFY_PAYLOAD_LIMIT};
use crate::wire::KeyBytes;

pub use crate::transport::listening::{RECONNECT_BACKOFF_MAX, RECONNECT_BACKOFF_MIN};

pub struct PgListenNotify {
    pub(super) pool: PgPool,
    pub(super) channel: ChannelName,
    pub(super) listener: Mutex<Option<PgListener>>,
    pub(super) consumed: AtomicBool,
    pub(super) health_tx: Mutex<Option<watch::Sender<bool>>>,
    pub(super) health_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for PgListenNotify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self
            .listener
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false);
        f.debug_struct("PgListenNotify")
            .field("channel", &self.channel)
            .field("listener_unclaimed", &held)
            .finish()
    }
}

impl PgListenNotify {
    pub fn channel(&self) -> &ChannelName {
        &self.channel
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn queue_usage(&self) -> Result<f64, EngineError> {
        let usage: f64 = sqlx::query_scalar("SELECT pg_notification_queue_usage()")
            .fetch_one(&self.pool)
            .await?;
        Ok(usage)
    }

    pub async fn fire_due(&self, batch: i64) -> Result<usize, EngineError> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(&format!(
            "SELECT id, noun, key FROM {TABLE_SCHEDULED_IMPACT} \
             WHERE at <= now() ORDER BY at, id LIMIT $1 FOR UPDATE SKIP LOCKED"
        ))
        .bind(batch)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut impacts = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(row.get::<Uuid, _>("id"));
            impacts.push(Impact::ResourceChanged {
                noun: NounName::new(row.get::<String, _>("noun"))?,
                key: serde_json::from_value::<KeyBytes>(row.get::<serde_json::Value, _>("key"))
                    .map_err(|source| EngineError::Decode {
                        what: KeyBytes::WHAT,
                        source,
                    })?,
                dims: Dims::EMPTY,
                cause: None,
            });
        }
        self.stage_in(&mut tx, &impacts).await?;
        sqlx::query(&format!(
            "DELETE FROM {TABLE_SCHEDULED_IMPACT} WHERE id = ANY($1)"
        ))
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(impacts.len())
    }
}

impl ImpactTransport for PgListenNotify {
    fn stage_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            for payload in encode(impacts, NOTIFY_PAYLOAD_LIMIT)? {
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(self.channel.as_str())
                    .bind(payload)
                    .execute(&mut *conn)
                    .await
                    .map_err(TransportError::Stage)?;
            }
            Ok(())
        })
    }

    fn schedule_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        noun: NounName,
        key: KeyBytes,
        at: Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO {TABLE_SCHEDULED_IMPACT} (id, at, noun, key) VALUES ($1, $2, $3, $4)"
            ))
            .bind(Uuid::now_v7())
            .bind(at)
            .bind(noun.as_str())
            .bind(sqlx::types::Json(&key))
            .execute(&mut *conn)
            .await
            .map_err(TransportError::Stage)?;
            Ok(())
        })
    }

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        self.stream()
    }
}
