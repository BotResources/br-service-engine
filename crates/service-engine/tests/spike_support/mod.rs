mod idle_runtime;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use br_core_auth::{AuthMethod, Passport, PassportClaims};
use futures_util::future::BoxFuture;
use idle_runtime::idle_runtime;
use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, AccumulatorRuntime, ChunkReader, ChunkSeq};
use service_engine::cohort::CohortKey;
use service_engine::delta::ErasedView;
use service_engine::erase::{ErasedLoadScope, ErasedPopulation, ErasedProjector, erase_projector};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{AccumulatorName, NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::principal::{Principal, PrincipalId};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::{KeyBytes, Noun};
use sqlx::PgPool;
use uuid::Uuid;

const LAZY_URL: &str = "postgresql://engine:engine@127.0.0.1:1/engine";

#[derive(Clone)]
pub struct Viewer {
    pub id: PrincipalId,
    pub tenant: Uuid,
    pub passport: Passport,
}

impl Viewer {
    pub fn new(tenant: Uuid) -> Self {
        let user_id = Uuid::now_v7();
        Self {
            id: PrincipalId::from(user_id),
            tenant,
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

pub struct Assignment;

impl Noun for Assignment {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("assignment");
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct AssignmentView {
    pub id: Uuid,
    pub tenant: Uuid,
    pub can_close: bool,
}

pub struct Assignments {
    pub open: BTreeSet<Uuid>,
}

impl Projector for Assignments {
    type Principal = Viewer;
    type Key = Uuid;
    type Facts = BTreeSet<Uuid>;
    type View = AssignmentView;

    fn name(&self) -> ProjectorName {
        ProjectorName::from_static("assignments")
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Assignment::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a Viewer,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move { Ok(Population::Keys(self.open.clone())) })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, Viewer>,
    ) -> BoxFuture<'a, Result<BTreeSet<Uuid>, EngineError>> {
        let keys: BTreeSet<Uuid> = scope.keys().iter().copied().collect();
        Box::pin(async move { Ok(keys) })
    }

    fn project(
        &self,
        facts: &BTreeSet<Uuid>,
        key: &Uuid,
        principal: &Viewer,
    ) -> Option<AssignmentView> {
        facts.contains(key).then(|| AssignmentView {
            id: *key,
            tenant: principal.tenant,
            can_close: self.open.contains(key),
        })
    }

    fn cohort(&self, principal: &Viewer) -> CohortKey {
        CohortKey::of(&[principal.tenant])
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct NoteKey {
    pub assignment: Uuid,
    pub seq: u32,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct NoteView {
    pub body: String,
}

pub struct Note;

impl Noun for Note {
    type Key = NoteKey;
    const NAME: NounName = NounName::from_static("note");
}

pub struct Notes {
    pub bodies: BTreeMap<NoteKey, String>,
}

impl Projector for Notes {
    type Principal = Viewer;
    type Key = NoteKey;
    type Facts = BTreeMap<NoteKey, String>;
    type View = NoteView;

    fn name(&self) -> ProjectorName {
        ProjectorName::from_static("notes")
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Note::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a Viewer,
    ) -> BoxFuture<'a, Result<Population<NoteKey>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Ordered {
                keys: self.bodies.keys().cloned().collect(),
                open_head: true,
            })
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<NoteKey> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, NoteKey, Viewer>,
    ) -> BoxFuture<'a, Result<BTreeMap<NoteKey, String>, EngineError>> {
        let wanted: Vec<NoteKey> = scope.keys().to_vec();
        Box::pin(async move {
            Ok(wanted
                .into_iter()
                .filter_map(|k| self.bodies.get(&k).map(|b| (k, b.clone())))
                .collect())
        })
    }

    fn project(
        &self,
        facts: &BTreeMap<NoteKey, String>,
        key: &NoteKey,
        _principal: &Viewer,
    ) -> Option<NoteView> {
        facts.get(key).map(|body| NoteView { body: body.clone() })
    }
}

pub struct Tokens;

impl Accumulator for Tokens {
    type Noun = Note;
    type Chunk = String;
    type State = String;

    fn name(&self) -> AccumulatorName {
        AccumulatorName::from_static("tokens")
    }

    fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
        state.push_str(&chunk);
    }
}

pub struct Fixture {
    pub projectors: Vec<Arc<dyn ErasedProjector<Viewer>>>,
    pub assignment: Uuid,
    pub note: NoteKey,
    pub viewer: Viewer,
    pub pg: PgPool,
    pub accumulators: AccumulatorRuntime,
}

pub fn fixture() -> Fixture {
    let assignment = Uuid::now_v7();
    let note = NoteKey { assignment, seq: 1 };
    let projectors: Vec<Arc<dyn ErasedProjector<Viewer>>> = vec![
        erase_projector(Assignments {
            open: BTreeSet::from([assignment]),
        }),
        erase_projector(Notes {
            bodies: BTreeMap::from([(note.clone(), "first note".to_string())]),
        }),
    ];
    let pg = PgPool::connect_lazy(LAZY_URL).expect("a lazy pool never dials");
    let accumulators = idle_runtime(pg.clone());
    Fixture {
        projectors,
        assignment,
        note,
        viewer: Viewer::new(Uuid::now_v7()),
        pg,
        accumulators,
    }
}

pub async fn render(
    projector: &Arc<dyn ErasedProjector<Viewer>>,
    pg: &PgPool,
    chunks: &ChunkReader,
    viewer: &Viewer,
) -> Vec<ErasedView> {
    let population = projector
        .populate(pg, &WindowParams::none(), viewer)
        .await
        .expect("populate");
    let keys: Vec<KeyBytes> = match population {
        ErasedPopulation::Keys(keys) => keys.into_iter().collect(),
        ErasedPopulation::Ordered { keys, .. } => keys,
        ErasedPopulation::Query(_) => Vec::new(),
    };
    let cohorts = [(projector.cohort(viewer), viewer)];
    let facts = projector
        .load(ErasedLoadScope::Bulk {
            pg,
            keys: &keys,
            cohorts: &cohorts,
            chunks,
        })
        .await
        .expect("load");
    keys.iter()
        .filter_map(|key| {
            projector
                .project(&facts, key, viewer)
                .expect("project")
                .map(|view| ErasedView::new(projector.name(), key.clone(), view))
        })
        .collect()
}
