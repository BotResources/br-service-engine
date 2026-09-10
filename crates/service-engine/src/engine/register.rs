use std::fmt::Display;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::Accumulator;
use crate::engine::Engine;
use crate::error::EngineError;
use crate::graphql::SliceFragment;
use crate::inbound::{Budgets, ReactionMessage, Subscription};
use crate::mirror::{MirrorReady, Project};
use crate::pipeline::Reaction;
use crate::presence::{Presence, PresenceHandle, PresenceKey};
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::transport::ImpactTransport;

impl<P: Principal> Engine<P> {
    pub fn register_schema_slice(&mut self, fragment: SliceFragment) -> Result<(), EngineError> {
        self.schema_slices.push(fragment);
        Ok(())
    }

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

    pub fn register_reaction_principal<F>(&mut self, resolver: F) -> Result<(), EngineError>
    where
        F: for<'a> Fn(
                &'a PgPool,
                br_core_integration::Actor,
            ) -> BoxFuture<'a, Result<P, EngineError>>
            + Send
            + Sync
            + 'static,
    {
        self.reaction_principal = Some(crate::principal::erase_reaction_resolver(resolver));
        Ok(())
    }

    pub fn register_principal_fact<F>(&mut self, loader: F) -> Result<(), EngineError>
    where
        F: for<'a> Fn(&'a PgPool, &'a mut P) -> BoxFuture<'a, Result<(), EngineError>>
            + Send
            + Sync
            + 'static,
    {
        self.with_registry(|registry| {
            registry.register_principal_fact(loader);
            Ok(())
        })
    }

    pub fn register_projector<Pr: Projector<Principal = P>>(
        &mut self,
        p: Pr,
    ) -> Result<(), EngineError> {
        self.with_registry(|registry| registry.add_projector(p))
    }

    pub fn register_view<V: crate::view::Projector<Principal = P>>(
        &mut self,
        view: V,
    ) -> Result<(), EngineError> {
        self.register_projector(crate::view::ViewProjector::new(view))
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
        let handle = mirror
            .with_reconcile_deadline(self.config.mirror_reconcile)
            .build_led(
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
        let leader = crate::offers::OfferLeader::new(
            self.config.pod_id.clone(),
            self.config.lease,
            self.config.beat,
        );
        let relay = crate::offers::OfferRelay::<O>::new(
            self.nats.clone(),
            leader,
            self.config.offer_reconcile,
        )
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

    pub fn blobs_configured(&self) -> bool {
        self.config.blob.is_some()
    }

    pub fn register_blobs<B: crate::blobs::Blobs>(
        &mut self,
        policy: crate::blobs::BlobPolicy,
    ) -> Result<(), EngineError> {
        self.blobs.register::<B>(policy)
    }

    pub fn declare_scopes(
        &mut self,
        manifest: crate::scopes::ScopeManifest,
    ) -> Result<(), EngineError> {
        let declaration = manifest.declaration()?;
        self.declared_scopes = Some(declaration);
        Ok(())
    }

    pub fn contribute_scopes(&mut self, scopes: &[&'static str]) -> Result<(), EngineError> {
        self.contributed_scopes.extend_from_slice(scopes);
        Ok(())
    }

    pub fn declare_contributed_scopes(&mut self) -> Result<(), EngineError> {
        if self.contributed_scopes.is_empty() {
            return Ok(());
        }
        let manifest = crate::scopes::ScopeManifest::of(&[self.contributed_scopes.as_slice()]);
        self.declare_scopes(manifest)
    }

    pub fn register_erasable<E: crate::erase::Erasable>(
        &mut self,
        erasable: E,
    ) -> Result<(), EngineError> {
        self.erasables
            .push(Arc::new(crate::erase::ErasableAdapter::new(erasable)));
        Ok(())
    }

    pub fn eraser(&self) -> crate::erase::Eraser<P> {
        crate::erase::Eraser::new(
            self.pg.clone(),
            self.transport.clone() as Arc<dyn ImpactTransport>,
            self.accumulators.clone(),
            Arc::new(self.offers.clone()),
            self.presence.handle(),
            self.blob_reader(),
            Arc::new(self.erasables.clone()),
            self.config.service.clone(),
            self.config.lock_timeout,
        )
    }

    pub async fn erase(
        &self,
        person: crate::erase::PersonId,
    ) -> Result<crate::erase::EraseOutcome, EngineError> {
        self.eraser().erase(person).await
    }
}
