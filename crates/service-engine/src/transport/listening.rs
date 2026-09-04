use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::stream::BoxStream;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::watch;

use crate::config::EngineConfig;
use crate::error::{EngineError, TransportError};
use crate::impact::TransportEvent;
use crate::transport::payload::Frame;
use crate::transport::pg::PgListenNotify;
use crate::transport::probe::ListenerProbe;
use crate::transport::reassemble::{Accepted, Reassembler};

pub const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
pub const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(5);

impl PgListenNotify {
    pub async fn connect(pool: PgPool, config: &EngineConfig) -> Result<Self, EngineError> {
        Self::connect_with_probe(pool, config, ListenerProbe::new()).await
    }

    pub async fn connect_with_probe(
        pool: PgPool,
        config: &EngineConfig,
        probe: ListenerProbe,
    ) -> Result<Self, EngineError> {
        config.validate()?;
        let mut listener = PgListener::connect_with(&pool)
            .await
            .map_err(TransportError::Listen)?;
        probe
            .run(&mut listener, &pool, config.listener_probe_timeout)
            .await?;
        listener
            .listen(config.channel.as_str())
            .await
            .map_err(TransportError::Listen)?;
        let (health_tx, health_rx) = watch::channel(true);
        Ok(Self {
            pool,
            channel: config.channel.clone(),
            listener: Mutex::new(Some(listener)),
            consumed: AtomicBool::new(false),
            health_tx: Mutex::new(Some(health_tx)),
            health_rx,
        })
    }

    pub fn listener_health(&self) -> watch::Receiver<bool> {
        self.health_rx.clone()
    }

    pub(super) fn stream(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        if self.consumed.swap(true, Ordering::SeqCst) {
            return Box::pin(futures_util::stream::once(async {
                Err(TransportError::ListenerConsumed)
            }));
        }
        let taken = self
            .listener
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let health = self
            .health_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let state = Listening {
            pool: self.pool.clone(),
            channel: self.channel.as_str().to_string(),
            established: taken.is_some(),
            listener: taken,
            reassembler: Reassembler::new(),
            backoff: RECONNECT_BACKOFF_MIN,
            repair: false,
            health,
        };
        Box::pin(futures_util::stream::unfold(state, next_event))
    }
}

struct Listening {
    pool: PgPool,
    channel: String,
    listener: Option<PgListener>,
    reassembler: Reassembler,
    established: bool,
    backoff: Duration,
    repair: bool,
    health: Option<watch::Sender<bool>>,
}

impl Listening {
    fn mark(&self, up: bool) {
        if let Some(health) = &self.health {
            let _ = health.send(up);
        }
    }
}

type Heard = Option<(Result<TransportEvent, TransportError>, Listening)>;

async fn next_event(mut state: Listening) -> Heard {
    if state.repair {
        state.repair = false;
        state.reassembler.clear();
        return Some((Ok(TransportEvent::Reconnected), state));
    }
    loop {
        if state.listener.is_none() {
            state.mark(false);
            match listening_connection(&state.pool, &state.channel).await {
                Ok(listener) => {
                    state.listener = Some(listener);
                    state.reassembler.clear();
                    state.mark(true);
                    if state.established {
                        return Some((Ok(TransportEvent::Reconnected), state));
                    }
                    state.established = true;
                }
                Err(TransportError::Listen(sqlx::Error::PoolClosed)) => return None,
                Err(_) => {
                    back_off(&mut state).await;
                    continue;
                }
            }
        }
        let listener = state
            .listener
            .as_mut()
            .expect("a listener was just established");
        let received = listener.try_recv().await;
        match received {
            Ok(Some(notification)) => {
                state.backoff = RECONNECT_BACKOFF_MIN;
                match heard(&mut state, notification.payload()) {
                    Ok(Accepted::Complete(impacts)) => {
                        return Some((Ok(TransportEvent::Impacts(impacts)), state));
                    }
                    Ok(Accepted::Buffered) => continue,
                    Ok(Accepted::BufferedAfterDrop) => {
                        return Some((Ok(TransportEvent::Reconnected), state));
                    }
                    Err(e) => {
                        state.repair = true;
                        return Some((Err(e), state));
                    }
                }
            }
            Ok(None) => {
                state.reassembler.clear();
                state.established = true;
                back_off(&mut state).await;
                return Some((Ok(TransportEvent::Reconnected), state));
            }
            Err(sqlx::Error::PoolClosed) => return None,
            Err(_) => {
                state.listener = None;
                state.mark(false);
                state.reassembler.clear();
                back_off(&mut state).await;
                continue;
            }
        }
    }
}

async fn back_off(state: &mut Listening) {
    tokio::time::sleep(state.backoff).await;
    state.backoff = (state.backoff * 2).min(RECONNECT_BACKOFF_MAX);
}

fn heard(state: &mut Listening, payload: &str) -> Result<Accepted, TransportError> {
    state.reassembler.accept(Frame::parse(payload)?)
}

async fn listening_connection(pool: &PgPool, channel: &str) -> Result<PgListener, TransportError> {
    let mut listener = PgListener::connect_with(pool)
        .await
        .map_err(TransportError::Listen)?;
    listener
        .listen(channel)
        .await
        .map_err(TransportError::Listen)?;
    Ok(listener)
}
