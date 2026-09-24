use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::error::EngineError;
use crate::inbound::budget::Budgets;
use crate::inbound::dispatch::DispatchError;
use crate::inbound::disposition::ReactionError;
use crate::inbound::subscription::{ReactionCoordinates, Subscription};
use crate::pipeline::Reaction;

pub trait ReactionMessage: Sized + Send + 'static {
    fn coordinates() -> ReactionCoordinates;
    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error>;
}

pub trait ReactionInvoker: Send + Sync + 'static {
    fn invoke<'a>(
        &'a self,
        cx: &'a mut Reaction<'a>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), DispatchError>>;
}

struct ReactionFn<M, H> {
    handler: H,
    _message: PhantomData<fn() -> M>,
}

impl<M, H, E> ReactionInvoker for ReactionFn<M, H>
where
    M: ReactionMessage,
    E: ReactionError,
    H: for<'r> Fn(&'r mut Reaction<'r>, M) -> BoxFuture<'r, Result<(), E>> + Send + Sync + 'static,
{
    fn invoke<'a>(
        &'a self,
        cx: &'a mut Reaction<'a>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            let message = M::decode(payload).map_err(|error| {
                DispatchError::terminal(format!(
                    "payload decode: {}",
                    crate::chain::describe(&error)
                ))
            })?;
            (self.handler)(cx, message)
                .await
                .map_err(|error| handler_fault(&error))
        })
    }
}

fn handler_fault<E: ReactionError>(error: &E) -> DispatchError {
    DispatchError::new(error.disposition(), crate::chain::describe(error))
}

pub struct ReactionEntry {
    subscription: Subscription,
    invoker: Arc<dyn ReactionInvoker>,
}

impl ReactionEntry {
    pub fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    pub fn invoker(&self) -> &Arc<dyn ReactionInvoker> {
        &self.invoker
    }
}

#[derive(Default)]
pub struct ReactionRegistry {
    entries: Vec<ReactionEntry>,
    durables: HashSet<String>,
}

impl ReactionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<M, H, E>(
        &mut self,
        durable: &str,
        handler: H,
        budgets: Budgets,
    ) -> Result<(), EngineError>
    where
        M: ReactionMessage,
        E: ReactionError,
        H: for<'r> Fn(&'r mut Reaction<'r>, M) -> BoxFuture<'r, Result<(), E>>
            + Send
            + Sync
            + 'static,
    {
        validate_durable(durable)?;
        if !self.durables.insert(durable.to_string()) {
            return Err(EngineError::Config(format!(
                "a reaction is already registered under the durable {durable}"
            )));
        }
        let subscription = Subscription::new(durable, M::coordinates()).with_budgets(budgets);
        let invoker: Arc<dyn ReactionInvoker> = Arc::new(ReactionFn::<M, H> {
            handler,
            _message: PhantomData,
        });
        self.entries.push(ReactionEntry {
            subscription,
            invoker,
        });
        Ok(())
    }

    pub fn subscriptions(&self) -> Vec<Subscription> {
        self.entries
            .iter()
            .map(|e| e.subscription.clone())
            .collect()
    }

    pub fn entries(&self) -> &[ReactionEntry] {
        &self.entries
    }

    pub fn invoker(&self, durable: &str) -> Option<&Arc<dyn ReactionInvoker>> {
        self.entries
            .iter()
            .find(|e| e.subscription.reaction == durable)
            .map(ReactionEntry::invoker)
    }
}

fn validate_durable(durable: &str) -> Result<(), EngineError> {
    let shaped = !durable.is_empty()
        && durable.len() <= 64
        && durable
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if shaped {
        Ok(())
    } else {
        Err(EngineError::InvalidName {
            kind: "reaction durable",
            value: durable.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use br_core_integration::{Aggregate, Bc, EventCoords, PastFact};

    #[derive(serde::Deserialize)]
    struct SampleMessage;

    impl ReactionMessage for SampleMessage {
        fn coordinates() -> ReactionCoordinates {
            ReactionCoordinates::Event(EventCoords {
                producer: Bc::new("orders").unwrap(),
                aggregate: Aggregate::new("order").unwrap(),
                fact: PastFact::new("confirmed").unwrap(),
                version: 1,
            })
        }

        fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
            serde_json::from_slice(payload)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("sample reaction failure")]
    struct SampleError;

    impl ReactionError for SampleError {
        fn disposition(&self) -> crate::inbound::disposition::Disposition {
            crate::inbound::disposition::Disposition::Retry
        }
    }

    fn handler<'r>(
        _cx: &'r mut Reaction<'r>,
        _m: SampleMessage,
    ) -> BoxFuture<'r, Result<(), SampleError>> {
        Box::pin(async { Ok(()) })
    }

    #[derive(Debug, thiserror::Error)]
    #[error("loading the roster")]
    struct RosterFault(#[source] EngineError);

    impl ReactionError for RosterFault {
        fn disposition(&self) -> crate::inbound::disposition::Disposition {
            crate::inbound::disposition::Disposition::Terminal
        }
    }

    #[test]
    fn a_reaction_fault_reaches_its_dispatch_detail_with_its_whole_source_chain() {
        let fault = RosterFault(EngineError::Config("the roster table is absent".into()));

        let dispatched = handler_fault(&fault);

        assert_eq!(
            dispatched.detail,
            "loading the roster: invalid configuration: the roster table is absent"
        );
        assert_eq!(
            dispatched.disposition,
            crate::inbound::disposition::Disposition::Terminal
        );
    }

    #[test]
    fn a_reaction_records_its_subscription_and_invoker() {
        let mut registry = ReactionRegistry::new();
        registry
            .register::<SampleMessage, _, _>("orders-confirmed", handler, Budgets::default())
            .unwrap();
        assert_eq!(registry.subscriptions().len(), 1);
        assert_eq!(registry.subscriptions()[0].reaction, "orders-confirmed");
        assert!(registry.invoker("orders-confirmed").is_some());
        assert!(registry.invoker("absent").is_none());
    }

    #[test]
    fn a_duplicate_durable_is_refused() {
        let mut registry = ReactionRegistry::new();
        registry
            .register::<SampleMessage, _, _>("orders-confirmed", handler, Budgets::default())
            .unwrap();
        let again = registry.register::<SampleMessage, _, _>(
            "orders-confirmed",
            handler,
            Budgets::default(),
        );
        assert!(again.is_err());
    }

    #[test]
    fn a_malformed_durable_is_refused() {
        let mut registry = ReactionRegistry::new();
        let bad = registry.register::<SampleMessage, _, _>(
            "orders.confirmed",
            handler,
            Budgets::default(),
        );
        assert!(matches!(bad, Err(EngineError::InvalidName { .. })));
    }
}
