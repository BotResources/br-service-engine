use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::EngineError;
use crate::inbound::consumer::InboundConsumer;
use crate::inbound::deadletter::DeadLetters;
use crate::inbound::dispatch::Dispatch;
use crate::inbound::subscription::{InboundConfig, Subscription};
use crate::inbound::supervisor::{InboundHealth, SupervisorConfig};
use crate::nats::Nats;
use std::sync::Arc;

pub struct InboundLoop {
    cancel: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl InboundLoop {
    pub async fn start(
        nats: Nats,
        subscriptions: Vec<Subscription>,
        dispatch: Arc<dyn Dispatch>,
        dead_letters: DeadLetters,
        config: InboundConfig,
        health: InboundHealth,
    ) -> Result<Self, EngineError> {
        let (cancel, _) = watch::channel(false);
        let supervision = SupervisorConfig::default();
        let mut tasks = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            let consumer = InboundConsumer::new(
                nats.clone(),
                subscription,
                dispatch.clone(),
                dead_letters.clone(),
                config.clone(),
            );
            consumer.open().await?;
            let stop = cancel.subscribe();
            tasks.push(tokio::spawn(consumer.run_supervised(
                stop,
                health.clone(),
                supervision,
            )));
        }
        Ok(Self { cancel, tasks })
    }

    pub fn stop(&self) {
        let _ = self.cancel.send(true);
    }

    pub async fn join(self) {
        for task in self.tasks {
            let _ = task.await;
        }
    }
}
