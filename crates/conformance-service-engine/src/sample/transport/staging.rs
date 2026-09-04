use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use service_engine::error::{EngineError, TransportError};
use service_engine::impact::{Impact, TransportEvent};
use service_engine::name::{ChannelName, NounName};
use service_engine::time::Timestamp;
use service_engine::transport::ImpactTransport;
use service_engine::wire::KeyBytes;
use sqlx::PgConnection;
use tokio::sync::Semaphore;

pub const SAMPLE_CHANNEL: ChannelName = ChannelName::from_static("sample_engine");

#[derive(Debug)]
pub struct StagingGate {
    entered: Semaphore,
    released: Semaphore,
}

impl Default for StagingGate {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            released: Semaphore::new(0),
        }
    }
}

impl StagingGate {
    pub async fn wait_until_staging(&self) {
        self.entered
            .acquire()
            .await
            .expect("the gate outlives the flush")
            .forget();
    }

    pub fn release(&self) {
        self.released.add_permits(1);
    }
}

#[derive(Debug, Default)]
pub struct StagingTransport {
    channel: Option<ChannelName>,
    staged: Mutex<Vec<Impact>>,
    scheduled: Mutex<Vec<(NounName, KeyBytes, Timestamp)>>,
    gate: Option<Arc<StagingGate>>,
}

impl StagingTransport {
    pub fn notifying(channel: ChannelName) -> Arc<Self> {
        Arc::new(Self {
            channel: Some(channel),
            ..Self::default()
        })
    }

    pub fn silent() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn gated() -> (Arc<Self>, Arc<StagingGate>) {
        let gate = Arc::new(StagingGate::default());
        let transport = Arc::new(Self {
            gate: Some(gate.clone()),
            ..Self::default()
        });
        (transport, gate)
    }

    pub fn staged(&self) -> Vec<Impact> {
        self.staged
            .lock()
            .expect("the staged log is readable")
            .clone()
    }

    pub fn scheduled(&self) -> Vec<(NounName, KeyBytes, Timestamp)> {
        self.scheduled
            .lock()
            .expect("the scheduled log is readable")
            .clone()
    }
}

impl ImpactTransport for StagingTransport {
    fn stage_in<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            if let Some(channel) = &self.channel {
                let payload = serde_json::to_string(impacts).map_err(TransportError::Payload)?;
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(channel.as_str())
                    .bind(payload)
                    .execute(&mut *conn)
                    .await
                    .map_err(TransportError::Stage)?;
            }
            self.staged
                .lock()
                .expect("the staged log is writable")
                .extend(impacts.iter().cloned());
            if let Some(gate) = &self.gate {
                gate.entered.add_permits(1);
                gate.released
                    .acquire()
                    .await
                    .expect("the gate outlives the flush")
                    .forget();
            }
            Ok(())
        })
    }

    fn schedule_in<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        noun: NounName,
        key: KeyBytes,
        at: Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            self.scheduled
                .lock()
                .expect("the scheduled log is writable")
                .push((noun, key, at));
            Ok(())
        })
    }

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        Box::pin(stream::empty())
    }
}
