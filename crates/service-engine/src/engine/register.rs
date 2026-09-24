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
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::transport::ImpactTransport;

impl<P: Principal> Engine<P> {
    pub fn register_schema_slice(&mut self, fragment: SliceFragment) -> Result<(), EngineError> {
        self.schema_slices.push(fragment);
        Ok(())
    }

    pub fn declare_root_prefix(
        &mut self,
        prefix: crate::graphql::RootPrefix,
    ) -> Result<(), EngineError> {
        match &self.root_prefix {
            Some(existing) if existing != &prefix => Err(EngineError::RootPrefixRedeclared {
                first: existing.snake().to_string(),
                second: prefix.snake().to_string(),
            }),
            _ => {
                self.root_prefix = Some(prefix);
                Ok(())
            }
        }
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
        mirror.validate()?;
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
        E: crate::inbound::ReactionError,
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
        E: crate::inbound::ReactionError,
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
}
