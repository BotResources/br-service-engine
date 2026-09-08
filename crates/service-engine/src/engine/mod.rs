mod run;

use std::sync::{Arc, Mutex, OnceLock};

use crate::nats::Nats;
use br_util_axum_readiness::ReadinessHandle;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::{Accumulator, AccumulatorRuntime, ChunkSeq, Durable};
use crate::boot::establish_transport_with_probe;
use crate::config::EngineConfig;
use crate::error::{AttachError, EngineError};
use crate::housekeeping::beat::Beat;
use crate::housekeeping::mirror::MirrorSupervisor;
use crate::inbound::{Budgets, ReactionMessage, ReactionRegistry, Subscription};
use crate::pipeline::Reaction;
use crate::presence::{Presence, PresenceHandle, PresenceKey, PresenceRegistry};
use crate::mirror::{MirrorReady, Project};
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::registry::RenderRegistry;
#[cfg(feature = "test-support")]
use crate::relay::Relay;
use crate::runtime::SessionRuntime;
use crate::session::{AttachRequest, SessionStream};
use crate::transport::probe::ListenerProbe;
use crate::transport::{ImpactTransport, PgListenNotify};
use crate::wire::Noun;
use futures_util::future::BoxFuture;
use std::fmt::Display;
use br_core_scope::ScopeDeclaration;

pub struct Engine<P: Principal> {
    config: EngineConfig,
    pg: PgPool,
    nats: Nats,
    transport: Arc<PgListenNotify>,
    readiness: ReadinessHandle,
    accumulators: Arc<AccumulatorRuntime>,
    registry: Mutex<Option<RenderRegistry<P>>>,
    render: OnceLock<Arc<SessionRuntime<P>>>,
    beat: Beat,
    mirrors: MirrorSupervisor,
    inbound_reactions: Mutex<ReactionRegistry>,
    presence: PresenceRegistry<P>,
    shutdown: Arc<tokio::sync::Notify>,
    declared_scopes: Option<ScopeDeclaration>,
}

impl<P: Principal> Engine<P> {
    pub async fn boot(
        config: EngineConfig,
        pg: PgPool,
        nats: Nats,
        readiness: ReadinessHandle,
    ) -> Result<Engine<P>, EngineError> {
        Self::boot_with_probe(config, pg, nats, readiness, ListenerProbe::new()).await
    }

    pub async fn boot_with_probe(
        config: EngineConfig,
        pg: PgPool,
        nats: Nats,
        readiness: ReadinessHandle,
        probe: ListenerProbe,
    ) -> Result<Engine<P>, EngineError> {
        config.validate()?;
        crate::observe::install_identity(&config);
        let transport =
            Arc::new(establish_transport_with_probe(pg.clone(), &config, &readiness, probe).await?);
        let accumulators = Arc::new(
            AccumulatorRuntime::new(
                pg.clone(),
                transport.clone() as Arc<dyn ImpactTransport>,
                config.chunk_retention,
            )
            .with_max_buffered_chunks(config.max_buffered_chunks)
            .with_fold_cache_capacity(config.fold_cache_capacity),
        );
        let beat = Beat::from_config(&config)?;
        Ok(Engine {
            config,
            pg,
            nats,
            transport,
            readiness,
            accumulators,
            registry: Mutex::new(Some(RenderRegistry::new())),
            render: OnceLock::new(),
            beat,
            mirrors: MirrorSupervisor::new(),
            inbound_reactions: Mutex::new(ReactionRegistry::new()),
            presence: PresenceRegistry::new(),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            declared_scopes: None,
        })
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

    pub fn register_projector<Pr: Projector<Principal = P>>(
        &mut self,
        p: Pr,
    ) -> Result<(), EngineError> {
        self.with_registry(|registry| registry.register_projector(p))
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
        let handle = mirror.build(
            self.nats.clone(),
            self.pg.clone(),
            self.transport.clone() as Arc<dyn ImpactTransport>,
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

    pub fn register_mutation<M, H>(&mut self, _handler: H) -> Result<(), EngineError> {
        Err(EngineError::NotYet {
            capability: "register_mutation",
        })
    }

    pub fn register_offer<O: crate::offer::Offer>(&mut self) -> Result<(), EngineError> {
        Err(EngineError::NotYet {
            capability: "register_offer",
        })
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

    pub fn readiness(&self) -> ReadinessHandle {
        self.readiness.clone()
    }

    pub fn shutdown_handle(&self) -> Arc<tokio::sync::Notify> {
        self.shutdown.clone()
    }

    pub(crate) fn render_runtime(&self) -> Arc<SessionRuntime<P>> {
        self.render
            .get_or_init(|| {
                let registry = self
                    .registry
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                    .expect("the render registry is assembled once, before the first attach");
                SessionRuntime::new(
                    self.config.clone(),
                    self.pg.clone(),
                    registry,
                    self.accumulators.reader().clone(),
                )
            })
            .clone()
    }

    pub async fn attach(&self, req: AttachRequest<P>) -> Result<SessionStream, AttachError> {
        self.render_runtime().attach(req).await
    }

    pub fn push_chunk<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
        seq: ChunkSeq,
        chunk: A::Chunk,
    ) -> Result<Durable, EngineError> {
        self.accumulators.push_chunk::<A>(key, seq, chunk)
    }

    pub async fn seal<A: Accumulator>(
        &self,
        tx: &mut PgConnection,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<(), EngineError> {
        self.accumulators.seal::<A>(tx, key).await
    }

    pub fn nats(&self) -> &Nats {
        &self.nats
    }

    fn with_registry(
        &mut self,
        f: impl FnOnce(&mut RenderRegistry<P>) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let mut guard = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_mut() {
            Some(registry) => f(registry),
            None => Err(EngineError::Config(
                "a component was registered after the render runtime was built".into(),
            )),
        }
    }
}

#[cfg(feature = "test-support")]
impl<P: Principal> Engine<P> {
    pub fn bind_noun<N: Noun>(&mut self) -> Result<(), EngineError> {
        self.with_registry(|registry| {
            registry.bind_noun::<N>();
            Ok(())
        })
    }

    pub fn register_relay<R: Relay>(&mut self, r: R) -> Result<(), EngineError> {
        self.beat.relays().register_erased(Arc::new(r))
    }

    #[cfg(feature = "test-support")]
    pub fn register_mirror_handle(
        &mut self,
        m: crate::mirror::MirrorHandle,
    ) -> Result<(), EngineError> {
        self.mirrors.register(m)
    }

    pub fn transport(&self) -> &dyn ImpactTransport {
        self.transport.as_ref()
    }

    pub fn transport_arc(&self) -> Arc<dyn ImpactTransport> {
        self.transport.clone()
    }

    pub fn accumulators(&self) -> &Arc<AccumulatorRuntime> {
        &self.accumulators
    }

    pub fn render(&self) -> Arc<SessionRuntime<P>> {
        self.render_runtime()
    }
}

impl<P: Principal> std::fmt::Debug for Engine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("pod", &self.config.pod_id)
            .field("mirrors", &self.mirrors.names())
            .finish_non_exhaustive()
    }
}
