use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::engine::Engine;
use crate::error::EngineError;
use crate::presence::{Presence, PresenceHandle, PresenceKey};
use crate::principal::Principal;
use crate::transport::ImpactTransport;

impl<P: Principal> Engine<P> {
    pub fn register_post_save_policy<A, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        A: crate::persistence::Aggregate,
        F: Fn(
                crate::pipeline::Saved<'_, A>,
                &mut crate::pipeline::PostSave<'_, '_>,
            ) -> Result<(), crate::pipeline::Refused>
            + Send
            + Sync
            + 'static,
    {
        self.post_save.register::<A, F>(policy)
    }

    pub fn require_post_save_policy<A>(&mut self) -> Result<(), EngineError>
    where
        A: crate::persistence::Aggregate,
    {
        self.policy_seams.require_save::<A>();
        Ok(())
    }

    pub fn register_post_delete_policy<A, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        A: crate::persistence::Aggregate,
        F: Fn(
                crate::pipeline::Saved<'_, A>,
                &mut crate::pipeline::PostSave<'_, '_>,
            ) -> Result<(), crate::pipeline::Refused>
            + Send
            + Sync
            + 'static,
    {
        self.post_delete.register::<A, F>(policy)
    }

    pub fn require_post_delete_policy<A>(&mut self) -> Result<(), EngineError>
    where
        A: crate::persistence::Aggregate,
    {
        self.policy_seams.require_delete::<A>();
        Ok(())
    }

    pub fn register_post_upload_policy<B, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        B: crate::blobs::Blobs,
        F: for<'a> Fn(
                crate::blobs::Uploaded<'a>,
                &'a mut crate::pipeline::PostSave<'_, '_>,
            ) -> BoxFuture<'a, Result<(), crate::pipeline::Refused>>
            + Send
            + Sync
            + 'static,
    {
        self.post_upload.register::<B, F>(policy)
    }

    pub fn require_post_upload_policy<B>(&mut self) -> Result<(), EngineError>
    where
        B: crate::blobs::Blobs,
    {
        self.policy_seams.require_upload::<B>();
        Ok(())
    }

    pub fn register_offer<O: crate::offer::Offer>(&mut self) -> Result<(), EngineError> {
        if !O::PREFIX.ends_with('/') {
            return Err(EngineError::Config(format!(
                "offer {} publishes prefix {:?} which must end with '/' so its manifest key sits \
                 outside the data prefix",
                O::NAME,
                O::PREFIX,
            )));
        }
        let leader = crate::offers::OfferLeader::new(self.config.pod_id.clone(), self.config.lease);
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

    pub fn register_offer_trigger<O, T>(&mut self) -> Result<(), EngineError>
    where
        O: crate::offer::Offer,
        T: crate::offer::OfferTrigger<O>,
    {
        self.offers.register_trigger::<O, T>()
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
