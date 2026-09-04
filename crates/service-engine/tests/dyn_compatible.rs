use std::collections::BTreeSet;

use br_core_auth::{AuthMethod, Passport, PassportClaims};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, ChunkSeq};
use service_engine::erase::{
    ErasedAccumulator, ErasedProjector, erase_accumulator, erase_projector,
};
use service_engine::error::{CronError, EngineError, RelayError, TransportError};
use service_engine::impact::{ForeignKey, Impact, TransportEvent};
use service_engine::name::{AccumulatorName, JobName, NounName, ProjectorName, RelayName};
use service_engine::population::{Inverse, Population};
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
use service_engine::projector::{LoadScope, Projector};
use service_engine::relay::{Claim, Drained, Relay};
use service_engine::session::WindowParams;
use service_engine::transport::ImpactTransport;
use service_engine::wire::{KeyBytes, Noun};
use service_engine::{CronJob, Schedule};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

const LAZY_URL: &str = "postgresql://engine:engine@127.0.0.1:1/engine";

#[derive(Clone)]
struct Viewer {
    id: PrincipalId,
    passport: Passport,
}

impl Viewer {
    fn new() -> Self {
        let user_id = Uuid::now_v7();
        Self {
            id: PrincipalId::from(user_id),
            passport: Passport::human(
                user_id,
                false,
                true,
                AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
        }
    }
}

impl Principal for Viewer {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

struct Rls;

impl RlsApplier<Viewer> for Rls {
    fn apply<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _principal: &'a Viewer,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }
}

struct Resolver;

impl PrincipalResolver<Viewer> for Resolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        current: &'a Viewer,
    ) -> BoxFuture<'a, Result<Option<Viewer>, EngineError>> {
        Box::pin(async move { Ok(Some(current.clone())) })
    }
}

struct Ticket;

impl Noun for Ticket {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("ticket");
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct TicketView {
    id: Uuid,
}

struct Tickets;

impl Projector for Tickets {
    type Principal = Viewer;
    type Key = Uuid;
    type Facts = Vec<Uuid>;
    type View = TicketView;

    fn name(&self) -> ProjectorName {
        ProjectorName::from_static("tickets")
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Ticket::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a Viewer,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async { Ok(Population::Keys(BTreeSet::new())) })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, Viewer>,
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        let keys = scope.keys().to_vec();
        Box::pin(async move { Ok(keys) })
    }

    fn project(&self, facts: &Vec<Uuid>, key: &Uuid, _principal: &Viewer) -> Option<TicketView> {
        facts.contains(key).then_some(TicketView { id: *key })
    }
}

struct Tokens;

impl Accumulator for Tokens {
    type Noun = Ticket;
    type Chunk = String;
    type State = String;

    fn name(&self) -> AccumulatorName {
        AccumulatorName::from_static("tokens")
    }

    fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
        state.push_str(&chunk);
    }
}

struct Outbox;

impl Relay for Outbox {
    fn name(&self) -> RelayName {
        RelayName::from_static("outbox")
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async { Ok(Drained::NOTHING) })
    }
}

struct Sweep;

impl CronJob for Sweep {
    fn name(&self) -> JobName {
        JobName::from_static("sweep")
    }

    fn schedule(&self) -> Schedule {
        Schedule::EveryBeats(60)
    }

    fn run<'a>(&'a self, _pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async { Ok(()) })
    }
}

struct NoTransport;

impl ImpactTransport for NoTransport {
    fn stage_in<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _impacts: &'a [Impact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn schedule_in<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _noun: NounName,
        _key: KeyBytes,
        _at: service_engine::Timestamp,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn listen(&self) -> BoxStream<'static, Result<TransportEvent, TransportError>> {
        Box::pin(futures_util::stream::empty())
    }
}

#[tokio::test]
async fn every_engine_trait_is_dyn_compatible_so_the_engine_can_hold_a_heterogeneous_registry() {
    let rls: Box<dyn RlsApplier<Viewer>> = Box::new(Rls);
    let resolver: Box<dyn PrincipalResolver<Viewer>> = Box::new(Resolver);
    let transport: Box<dyn ImpactTransport> = Box::new(NoTransport);
    let relay: Box<dyn Relay> = Box::new(Outbox);
    let job: Box<dyn CronJob> = Box::new(Sweep);
    let projector: Box<
        dyn Projector<Principal = Viewer, Key = Uuid, Facts = Vec<Uuid>, View = TicketView>,
    > = Box::new(Tickets);
    let accumulator: Box<dyn Accumulator<Noun = Ticket, Chunk = String, State = String>> =
        Box::new(Tokens);

    let pg = PgPool::connect_lazy(LAZY_URL).expect("a lazy pool never dials");
    let viewer = Viewer::new();

    assert_eq!(relay.name(), RelayName::from_static("outbox"));
    assert_eq!(job.schedule(), Schedule::EveryBeats(60));
    assert_eq!(projector.name(), ProjectorName::from_static("tickets"));
    assert_eq!(accumulator.name(), AccumulatorName::from_static("tokens"));
    assert!(resolver.resolve(&pg, &viewer).await.unwrap().is_some());

    let mut impacts = transport.listen();
    assert!(futures_util::StreamExt::next(&mut impacts).await.is_none());
    drop(rls);
}

#[test]
fn the_erased_registry_holds_projectors_and_accumulators_behind_one_trait_object() {
    let projectors: Vec<std::sync::Arc<dyn ErasedProjector<Viewer>>> =
        vec![erase_projector(Tickets)];
    let accumulators: Vec<std::sync::Arc<dyn ErasedAccumulator>> = vec![erase_accumulator(Tokens)];

    assert_eq!(projectors[0].name(), ProjectorName::from_static("tickets"));
    assert_eq!(projectors[0].nouns(), &[NounName::from_static("ticket")]);
    assert_eq!(accumulators[0].noun(), NounName::from_static("ticket"));
}
