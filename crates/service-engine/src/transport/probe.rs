use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::{EngineError, TransportError};

pub const PROBE: &str = "listener";
pub const PROBE_CHANNEL_PREFIX: &str = "service_engine_probe_";

pub const POOLER_REASON: &str = "the impact listener never heard its own probe: LISTEN is session \
                                 state, so the engine must hold a direct connection to the \
                                 PostgreSQL primary and never a transaction-pooling pooler";

#[derive(Debug, Clone)]
pub struct ListenerProbe {
    channel: String,
    fire_channel: String,
    token: String,
}

impl Default for ListenerProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl ListenerProbe {
    pub fn new() -> Self {
        let id = Uuid::now_v7().simple().to_string();
        let channel = format!("{PROBE_CHANNEL_PREFIX}{id}");
        Self {
            fire_channel: channel.clone(),
            channel,
            token: Uuid::now_v7().simple().to_string(),
        }
    }

    pub fn deaf() -> Self {
        let elsewhere = format!("{PROBE_CHANNEL_PREFIX}{}", Uuid::now_v7().simple());
        Self {
            fire_channel: elsewhere,
            ..Self::new()
        }
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub async fn run(
        &self,
        listener: &mut PgListener,
        notifier: &PgPool,
        timeout: Duration,
    ) -> Result<(), EngineError> {
        self.arm(listener).await?;
        let outcome = async {
            self.fire(notifier).await?;
            self.hear(listener, timeout).await
        }
        .await;
        let disarmed = self.disarm(listener).await;
        outcome.and(disarmed)
    }

    pub async fn arm(&self, listener: &mut PgListener) -> Result<(), EngineError> {
        listener
            .listen(&self.channel)
            .await
            .map_err(TransportError::Listen)?;
        Ok(())
    }

    pub async fn fire(&self, notifier: &PgPool) -> Result<(), EngineError> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&self.fire_channel)
            .bind(&self.token)
            .execute(notifier)
            .await
            .map_err(TransportError::Stage)?;
        Ok(())
    }

    pub async fn hear(
        &self,
        listener: &mut PgListener,
        timeout: Duration,
    ) -> Result<(), EngineError> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(EngineError::ProbeTimeout {
                    probe: PROBE,
                    timeout,
                });
            }
            match tokio::time::timeout(left, listener.try_recv()).await {
                Err(_) => {
                    return Err(EngineError::ProbeTimeout {
                        probe: PROBE,
                        timeout,
                    });
                }
                Ok(Err(e)) => return Err(TransportError::Listen(e).into()),
                Ok(Ok(None)) => continue,
                Ok(Ok(Some(notification))) => {
                    if notification.channel() != self.channel {
                        return Err(EngineError::ProbeInterference {
                            probe: PROBE,
                            channel: notification.channel().to_string(),
                        });
                    }
                    if notification.payload() == self.token {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn disarm(&self, listener: &mut PgListener) -> Result<(), EngineError> {
        listener
            .unlisten(&self.channel)
            .await
            .map_err(TransportError::Listen)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_probe_owns_a_private_channel_within_the_postgres_identifier_budget() {
        let one = ListenerProbe::new();
        let two = ListenerProbe::new();
        assert_ne!(one.channel(), two.channel());
        assert!(one.channel().starts_with(PROBE_CHANNEL_PREFIX));
        assert!(one.channel().len() <= 63);
        assert_ne!(one.token, two.token);
    }
}
