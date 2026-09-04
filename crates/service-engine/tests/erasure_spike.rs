mod spike_support;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize, Serializer};
use service_engine::ChunkSeq;
use service_engine::cohort::CohortKey;
use service_engine::erase::{
    ErasedAccumulator, ErasedInverse, ErasedLoadScope, ErasedPopulation, erase_accumulator,
    erase_projector,
};
use service_engine::error::{DecodeError, EngineError};
use service_engine::impact::{Dims, ForeignKey, Impact};
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Interest, Inverse, Population, WindowQuery};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::{KeyBytes, Noun};
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use spike_support::{
    Assignment, AssignmentView, Assignments, Fixture, NoteView, Notes, Tokens, Viewer, fixture,
    render,
};

#[tokio::test]
async fn one_erased_registry_renders_two_projectors_with_different_key_facts_and_view_types() {
    let Fixture {
        projectors,
        assignment,
        note,
        viewer,
        pg,
        accumulators,
    } = fixture();

    let assignments = render(&projectors[0], &pg, accumulators.reader(), &viewer).await;
    let notes = render(&projectors[1], &pg, accumulators.reader(), &viewer).await;

    assert_eq!(assignments.len(), 1);
    assert_eq!(notes.len(), 1);

    let (key, view) = assignments[0]
        .decode::<Assignments>()
        .expect("the typed assignment view comes back");
    assert_eq!(key, assignment);
    assert_eq!(
        view,
        AssignmentView {
            id: assignment,
            tenant: viewer.tenant,
            can_close: true,
        }
    );

    let impact = Impact::resource::<Assignment>(&assignment, Dims::EMPTY).unwrap();
    let Impact::ResourceChanged {
        noun,
        key: impacted,
        ..
    } = &impact
    else {
        panic!("expected a ResourceChanged");
    };
    assert_eq!(noun, &Assignment::NAME);
    assert_eq!(impacted, &assignments[0].key);

    let (key, view) = notes[0]
        .decode::<Notes>()
        .expect("the typed note view comes back");
    assert_eq!(key, note);
    assert_eq!(
        view,
        NoteView {
            body: "first note".into()
        }
    );
}

#[tokio::test]
async fn decoding_an_erased_view_with_the_wrong_projector_fails_typed() {
    let Fixture {
        projectors,
        viewer,
        pg,
        accumulators,
        ..
    } = fixture();
    let assignments = render(&projectors[0], &pg, accumulators.reader(), &viewer).await;

    let by_shape = assignments[0].decode::<Notes>();
    assert!(matches!(
        by_shape,
        Err(DecodeError::Key(_) | DecodeError::View(_))
    ));

    let by_name = assignments[0].decode_from(&Notes {
        bodies: BTreeMap::new(),
    });
    assert!(matches!(by_name, Err(DecodeError::Projector { .. })));
}

#[tokio::test]
async fn facts_loaded_for_one_projector_are_refused_by_another() {
    let Fixture {
        projectors,
        viewer,
        pg,
        accumulators,
        ..
    } = fixture();
    let keys = match projectors[0]
        .populate(&pg, &WindowParams::none(), &viewer)
        .await
        .unwrap()
    {
        ErasedPopulation::Keys(keys) => keys.into_iter().collect::<Vec<_>>(),
        _ => panic!("the assignments window is a key set"),
    };
    let cohorts = [(projectors[0].cohort(&viewer), &viewer)];
    let facts = projectors[0]
        .load(ErasedLoadScope::Bulk {
            pg: &pg,
            keys: &keys,
            cohorts: &cohorts,
            chunks: accumulators.reader(),
        })
        .await
        .unwrap();

    let wrong = projectors[1].project(&facts, &keys[0], &viewer);
    assert!(matches!(wrong, Err(EngineError::FactsMismatch { .. })));
}

#[test]
fn an_erased_accumulator_folds_its_chunks_through_a_type_erased_state() {
    let accumulator: Arc<dyn ErasedAccumulator> = erase_accumulator(Tokens);
    let mut state = accumulator.init_state();
    for (seq, token) in ["he", "llo"].into_iter().enumerate() {
        accumulator
            .fold(
                &mut state,
                ChunkSeq::new(seq as u64).unwrap(),
                &serde_json::Value::String(token.to_string()),
            )
            .expect("fold");
    }
    assert_eq!(state.downcast_ref::<String>().unwrap(), "hello");

    let malformed = accumulator.fold(
        &mut state,
        ChunkSeq::new(2).unwrap(),
        &serde_json::Value::Number(7.into()),
    );
    assert!(matches!(
        malformed,
        Err(EngineError::Decode { what: "chunk", .. })
    ));
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Deserialize)]
struct UnencodableKey(u32);

impl Serialize for UnencodableKey {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this key refuses to encode"))
    }
}

struct Ghost;

impl Noun for Ghost {
    type Key = UnencodableKey;
    const NAME: NounName = NounName::from_static("ghost");
}

struct Ghosts;

impl Projector for Ghosts {
    type Principal = Viewer;
    type Key = UnencodableKey;
    type Facts = ();
    type View = ();

    fn name(&self) -> ProjectorName {
        ProjectorName::from_static("ghosts")
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Ghost::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a Viewer,
    ) -> BoxFuture<'a, Result<Population<UnencodableKey>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Query(WindowQuery::new(
                Interest::new().on_noun(Ghost::NAME, Dims::EMPTY),
                Arc::new(|_key, _impact| true),
            )))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<UnencodableKey> {
        Inverse::Keys(BTreeSet::from([UnencodableKey(1)]))
    }

    fn load<'a>(
        &'a self,
        _scope: LoadScope<'a, UnencodableKey, Viewer>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move { Ok(()) })
    }

    fn project(&self, _facts: &(), _key: &UnencodableKey, _principal: &Viewer) -> Option<()> {
        None
    }

    fn cohort(&self, principal: &Viewer) -> CohortKey {
        CohortKey::principal(principal.id)
    }
}

#[test]
fn an_inverse_whose_keys_cannot_be_erased_reports_the_error_instead_of_re_rendering_nothing() {
    let ghosts = erase_projector(Ghosts);
    let foreign = ForeignKey::new("identity.user", "u1").unwrap();

    match ghosts.inverse(&foreign) {
        Err(EngineError::Encode { what: "key", .. }) => {}
        Ok(ErasedInverse::None) => {
            panic!("an unencodable inverse must not read as nothing to re-render")
        }
        other => panic!("expected a typed encode error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_query_predicate_given_a_key_it_cannot_decode_reports_the_error_instead_of_false() {
    let ghosts = erase_projector(Ghosts);
    let viewer = Viewer::new(uuid::Uuid::now_v7());
    let pg = PgPool::connect_lazy("postgresql://engine:engine@127.0.0.1:1/engine").unwrap();

    let ErasedPopulation::Query(query) = ghosts
        .populate(&pg, &WindowParams::none(), &viewer)
        .await
        .expect("the ghost window is a query")
    else {
        panic!("expected a query population");
    };

    let alien = KeyBytes::encode(&"not a ghost key").unwrap();
    let verdict = (query.predicate())(
        &alien,
        &Impact::foreign(ForeignKey::new("x.y", "z").unwrap()),
    );

    assert!(matches!(
        verdict,
        Err(EngineError::Decode { what: "key", .. })
    ));
}
