use std::fmt::Display;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::accumulator::Accumulator;
use crate::engine::Engine;
use crate::error::EngineError;
use crate::inbound::{Budgets, ReactionMessage, Subscription};
use crate::mirror::{MirrorReady, Project};
use crate::pipeline::Reaction;
use crate::presence::{Presence, PresenceHandle, PresenceKey};
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::transport::ImpactTransport;

impl<P: Principal> Engine<P> {
    pub fn register_rls<R: RlsApplier<P>>(&mut self, r: R) -> Result<(), EngineError> {
        self.with_registry(|registry| {
            registry.register_rls(r);
            Ok(())
        })
    }

    pub fn register_principal_resolver<R: PrincipalResolver<P>>(
        &mut self,
        r: R,
    ) -> Result<(), EngineError> {
        self.with_registry(|registry| {
            registry.register_principal_resolver(r);
            Ok(())
        })
    }

    pub fn register_projector<Pr: Projector<Principal = P>>(
        &mut self,
        p: Pr,
    ) -> Result<(), EngineError> {
        self.with_registry(|registry| registry.add_projector(p))
    }

    pub fn register_accumulator<A: Accumulator>(&mut self, a: A) -> Result<(), EngineError> {
        self.with_registry(|registry| {
            registry.bind_noun::<A::Noun>();
            Ok(())
        })?;
        self.accumulators.register(a)
    }

    pub fn register_cron<J: crate::cron::CronJob>(&mut self, j: J) -> Result<(), EngineError> {
        self.beat
            .cron()
            .register_erased(Arc::new(j))
            .map_err(|error| EngineError::Service(Box::new(error)))
    }

    pub fn register_mirror<K, Pr>(&mut self, mirror: MirrorReady<K, Pr>) -> Result<(), EngineError>
    where
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        Pr: Project<K>,
    {
        let handle = mirror.build_led(
            self.nats.clone(),
            self.pg.clone(),
            self.transport.clone() as Arc<dyn ImpactTransport>,
            crate::mirror::MirrorLeader::new(
                self.config.pod_id.clone(),
                self.config.lease,
                self.config.beat,
            ),
        );
        self.mirrors.register(handle)
    }

    pub fn register_reaction<M, H, E>(
        &mut self,
        durable: &str,
        handler: H,
    ) -> Result<(), EngineError>
    where
        M: ReactionMessage,
        E: crate::inbound::ReactionError + Display + Send + 'static,
        H: for<'r> Fn(&'r mut Reaction<'r>, M) -> BoxFuture<'r, Result<(), E>>
            + Send
            + Sync
            + 'static,
    {
        self.register_reaction_with_budgets::<M, H, E>(durable, handler, Budgets::default())
    }

    pub fn register_reaction_with_budgets<M, H, E>(
        &mut self,
        durable: &str,
        handler: H,
        budgets: Budgets,
    ) -> Result<(), EngineError>
    where
        M: ReactionMessage,
        E: crate::inbound::ReactionError + Display + Send + 'static,
        H: for<'r> Fn(&'r mut Reaction<'r>, M) -> BoxFuture<'r, Result<(), E>>
            + Send
            + Sync
            + 'static,
    {
        self.inbound_reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .register::<M, H, E>(durable, handler, budgets)
    }

    pub fn inbound_subscriptions(&self) -> Vec<Subscription> {
        self.inbound_reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .subscriptions()
    }

    pub fn register_offer<O: crate::offer::Offer>(&mut self) -> Result<(), EngineError> {
        let relay =
            crate::offers::OfferRelay::<O>::new(self.nats.clone(), self.config.offer_reconcile)
                .map_err(|error| EngineError::Service(Box::new(error)))?;
        self.beat.relays().register_erased(Arc::new(relay))?;
        self.offers.register::<O>();
        Ok(())
    }

    pub fn register_presence<Pr: Presence>(
        &mut self,
        ttl: std::time::Duration,
    ) -> Result<(), EngineError> {
        let store = self.presence.register::<Pr>(ttl);
        self.with_registry(|registry| {
            registry.bind_noun::<Pr::Noun>();
            registry.register_projector(crate::presence::PresenceProjector::<P, Pr>::new(store))
        })
    }

    pub fn presence_handle(&self) -> PresenceHandle<P> {
        self.presence.handle()
    }

    pub async fn present<Pr: Presence>(
        &self,
        key: &PresenceKey<Pr>,
        value: &Pr::Value,
    ) -> Result<(), EngineError> {
        self.presence.handle().present::<Pr>(key, value).await
    }

    pub fn register_blobs<B: crate::blobs::Blobs>(
        &mut self,
        _policy: crate::blobs::BlobPolicy,
    ) -> Result<(), EngineError> {
        Err(EngineError::NotYet {
            capability: "register_blobs",
        })
    }

    pub fn declare_scopes(
        &mut self,
        manifest: crate::scopes::ScopeManifest,
    ) -> Result<(), EngineError> {
        let declaration = manifest.declaration()?;
        self.declared_scopes = Some(declaration);
        Ok(())
    }

    pub async fn erase(&self, _person: crate::erase::PersonId) -> Result<(), EngineError> {
        Err(EngineError::NotYet {
            capability: "erase",
        })
    }
}
