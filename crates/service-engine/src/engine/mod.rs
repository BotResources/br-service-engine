pub mod boot;

mod blobs;
mod lane_a;
mod loops;
mod mutate;
mod register;
mod run;
mod serve;
mod shutdown;
mod wiring;

pub use blobs::BlobReader;

use std::sync::{Arc, Mutex, OnceLock};

use crate::nats::Nats;
use crate::readiness::ReadinessHandle;
use sqlx::PgPool;

use crate::accumulator::{Accumulator, AccumulatorRuntime, ChunkSeq, Durable};
use crate::blobs::BlobRegistry;
use crate::boot::establish_transport_with_probe;
use crate::config::EngineConfig;
use crate::erase::ErasedErasable;
use crate::error::{AttachError, EngineError};
use crate::graphql::SliceFragment;
use crate::housekeeping::beat::Beat;
use crate::housekeeping::mirror::MirrorSupervisor;
use crate::inbound::ReactionRegistry;
use crate::offers::OfferStagers;
use crate::pipeline::MutationRegistry;
use crate::pipeline::{AggregatePolicies, PolicySeams};
use crate::presence::PresenceRegistry;
use crate::principal::Principal;
use crate::registry::RenderRegistry;
use crate::runtime::SessionRuntime;
use crate::session::{AttachRequest, SessionStream};
use crate::transport::probe::ListenerProbe;
use crate::transport::{ImpactTransport, PgListenNotify};
use crate::wire::Noun;
use br_core_scope::ScopeDeclaration;

pub struct Settle<P: Principal> {
    render: Arc<SessionRuntime<P>>,
    accumulators: Arc<AccumulatorRuntime>,
}

impl<P: Principal> Settle<P> {
    pub async fn settle(&self, quiet: std::time::Duration, deadline: std::time::Duration) {
        let _ = self.accumulators.flush_once().await;
        self.render.settle(quiet, deadline).await;
    }
}

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
    mutations: MutationRegistry<P>,
    offers: OfferStagers,
    post_save: AggregatePolicies,
    post_delete: AggregatePolicies,
    post_upload: crate::blobs::policy::UploadPolicies,
    policy_seams: PolicySeams,
    presence: PresenceRegistry<P>,
    blobs: BlobRegistry,
    erasables: Vec<Arc<dyn ErasedErasable>>,
    schema_slices: Vec<SliceFragment>,
    root_prefix: Option<crate::graphql::RootPrefix>,
    schema_sdl: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
    ws_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    declared_scopes: Option<ScopeDeclaration>,
    contributed_scopes: Vec<&'static str>,
    reaction_principal: Option<Arc<dyn crate::principal::ReactionPrincipalResolver>>,
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
        let nats = nats.with_publish_ack_timeout(config.publish_ack_timeout);
        let transport =
            Arc::new(establish_transport_with_probe(pg.clone(), &config, &readiness, probe).await?);
        let accumulators = Arc::new(
            AccumulatorRuntime::new(
                pg.clone(),
                transport.clone() as Arc<dyn ImpactTransport>,
                config.seal_retention,
            )
            .with_max_buffered_chunks(config.max_buffered_chunks)
            .with_fold_cache_capacity(config.fold_cache_capacity)
            .with_flush_lock_timeout(config.lock_timeout),
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
            mutations: MutationRegistry::new(),
            offers: OfferStagers::default(),
            post_save: AggregatePolicies::default(),
            post_delete: AggregatePolicies::default(),
            post_upload: crate::blobs::policy::UploadPolicies::default(),
            policy_seams: PolicySeams::default(),
            presence: PresenceRegistry::new(),
            blobs: BlobRegistry::new(),
            erasables: Vec::new(),
            schema_slices: Vec::new(),
            root_prefix: None,
            schema_sdl: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            ws_shutdown: Arc::new(tokio::sync::watch::channel(false).0),
            declared_scopes: None,
            contributed_scopes: Vec::new(),
            reaction_principal: None,
        })
    }

    pub fn readiness(&self) -> ReadinessHandle {
        self.readiness.clone()
    }

    pub fn shutdown_handle(&self) -> Arc<tokio::sync::Notify> {
        self.shutdown.clone()
    }

    pub(crate) fn ws_shutdown_sender(&self) -> Arc<tokio::sync::watch::Sender<bool>> {
        self.ws_shutdown.clone()
    }

    pub(crate) fn ws_shutdown_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        self.ws_shutdown.subscribe()
    }

    pub fn set_schema_sdl(&mut self, sdl: String) {
        self.schema_sdl = Some(sdl);
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
                let mut lanes = Vec::new();
                if self.accumulators.registered() > 0 {
                    lanes.push(crate::lanes::Lane::Accumulated);
                }
                if !self.presence.is_empty() {
                    lanes.push(crate::lanes::Lane::Presence);
                }
                SessionRuntime::new(
                    self.config.clone(),
                    self.pg.clone(),
                    registry,
                    self.accumulators.reader().clone(),
                    lanes,
                )
            })
            .clone()
    }

    pub async fn attach(&self, req: AttachRequest<P>) -> Result<SessionStream, AttachError> {
        self.render_runtime().attach(req).await
    }

    pub fn lane_notices(
        &self,
    ) -> impl futures_util::Stream<Item = crate::lanes::LaneNotice> + use<P> {
        let channel = self.render_runtime().lane_channel();
        crate::lanes::notices(channel.receiver(), channel.lanes())
    }

    pub fn push_chunk<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
        seq: ChunkSeq,
        chunk: A::Chunk,
    ) -> Result<Durable, EngineError> {
        self.accumulators.push_chunk::<A>(key, seq, chunk)
    }

    pub fn nats(&self) -> &Nats {
        &self.nats
    }

    pub fn accumulator_handle(&self) -> Arc<AccumulatorRuntime> {
        self.accumulators.clone()
    }

    pub fn settle_handle(&self) -> Settle<P> {
        Settle {
            render: self.render_runtime(),
            accumulators: self.accumulators.clone(),
        }
    }

    pub(crate) fn with_registry(
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

    pub fn register_relay<R: crate::relay::Relay>(&mut self, r: R) -> Result<(), EngineError> {
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
