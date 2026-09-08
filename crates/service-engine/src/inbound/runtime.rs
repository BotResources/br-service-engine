use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::error::EngineError;
use crate::inbound::consumer::InboundConsumer;
use crate::inbound::deadletter::DeadLetters;
use crate::inbound::dispatch::Dispatch;
use crate::inbound::subscription::{InboundConfig, Subscription};
use crate::nats::Nats;

pub struct InboundLoop {
    cancel: Arc<Notify>,
    tasks: Vec<JoinHandle<Result<(), EngineError>>>,
}

impl InboundLoop {
    pub async fn start(
        nats: Nats,
        subscriptions: Vec<Subscription>,
        dispatch: Arc<dyn Dispatch>,
        dead_letters: DeadLetters,
        config: InboundConfig,
    ) -> Result<Self, EngineError> {
        let cancel = Arc::new(Notify::new());
        let mut tasks = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            let consumer = InboundConsumer::new(
                nats.clone(),
                subscription,
                dispatch.clone(),
                dead_letters.clone(),
                config.clone(),
            );
            let opened = consumer.open().await?;
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(consumer.serve(opened, cancel)));
        }
        Ok(Self { cancel, tasks })
    }

    pub fn stop(&self) {
        self.cancel.notify_waiters();
    }

    pub async fn join(self) {
        for task in self.tasks {
            let _ = task.await;
        }
    }
}
