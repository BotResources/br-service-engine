mod run;

use std::sync::{Arc, Mutex, OnceLock};

use br_util_axum_readiness::ReadinessHandle;
use br_util_nats_fabric::Fabric;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::{Accumulator, AccumulatorRuntime, ChunkSeq, Durable};
use crate::boot::establish_transport_with_probe;
use crate::config::EngineConfig;
use crate::error::{AttachError, EngineError};
use crate::housekeeping::beat::Beat;
use crate::housekeeping::mirror::MirrorSupervisor;
use crate::mirror::MirrorHandle;
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::registry::RenderRegistry;
use crate::relay::Relay;
use crate::runtime::SessionRuntime;
use crate::session::{AttachRequest, SessionStream};
use crate::transport::probe::ListenerProbe;
use crate::transport::{ImpactTransport, PgListenNotify};
use crate::wire::Noun;

pub struct Engine<P: Principal> {
    config: EngineConfig,
    pg: PgPool,
    fabric: Fabric,
    transport: Arc<PgListenNotify>,
    readiness: ReadinessHandle,
    accumulators: Arc<AccumulatorRuntime>,
    registry: Mutex<Option<RenderRegistry<P>>>,
    render: OnceLock<Arc<SessionRuntime<P>>>,
    beat: Beat,
    mirrors: MirrorSupervisor,
    shutdown: Arc<tokio::sync::Notify>,
}

impl<P: Principal> Engine<P> {
    pub async fn boot(
        config: EngineConfig,
        pg: PgPool,
        fabric: Fabric,
        readiness: ReadinessHandle,
    ) -> Result<Engine<P>, EngineError> {
        Self::boot_with_probe(config, pg, fabric, readiness, ListenerProbe::new()).await
    }

    pub async fn boot_with_probe(
        config: EngineConfig,
        pg: PgPool,
        fabric: Fabric,
        readiness: ReadinessHandle,
        probe: ListenerProbe,
    ) -> Result<Engine<P>, EngineError> {
        config.validate()?;
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
            fabric,
            transport,
            readiness,
            accumulators,
            registry: Mutex::new(Some(RenderRegistry::new())),
            render: OnceLock::new(),
            beat,
            mirrors: MirrorSupervisor::new(),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub fn bind_noun<N: Noun>(&mut self) -> Result<(), EngineError> {
        self.with_registry(|registry| {
            registry.bind_noun::<N>();
            Ok(())
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

    pub fn register_relay<R: Relay>(&mut self, r: R) -> Result<(), EngineError> {
        self.beat.relays().register_erased(Arc::new(r))
    }

    pub fn register_cron<J: crate::cron::CronJob>(&mut self, j: J) -> Result<(), EngineError> {
        self.beat
            .cron()
            .register_erased(Arc::new(j))
            .map_err(|error| EngineError::Service(Box::new(error)))
    }

    pub fn register_mirror(&mut self, m: MirrorHandle) -> Result<(), EngineError> {
        self.mirrors.register(m)
    }

    pub fn readiness(&self) -> ReadinessHandle {
        self.readiness.clone()
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

    pub fn shutdown_handle(&self) -> Arc<tokio::sync::Notify> {
        self.shutdown.clone()
    }

    pub fn render(&self) -> Arc<SessionRuntime<P>> {
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
        self.render().attach(req).await
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

    pub fn fabric(&self) -> &Fabric {
        &self.fabric
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

impl<P: Principal> std::fmt::Debug for Engine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("pod", &self.config.pod_id)
            .field("mirrors", &self.mirrors.names())
            .finish_non_exhaustive()
    }
}
