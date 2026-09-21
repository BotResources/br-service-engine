use std::collections::BTreeSet;
use std::marker::PhantomData;

use async_graphql::SimpleObject;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::delta::{Delta, ErasedView, Revision};
use crate::error::EngineError;
use crate::impact::ForeignKey;
use crate::name::{NounName, ProjectorName};
use crate::population::{Inverse, Population};
use crate::principal::Principal;
use crate::projector::{LoadScope, Projector};
use crate::session::WindowParams;
use crate::test_support::TestPrincipal;
use crate::wire::{KeyBytes, ViewBytes};

#[derive(Clone, PartialEq, Serialize, Deserialize, SimpleObject)]
struct WidgetView {
    id: i32,
}

struct Widgets<P>(PhantomData<fn() -> P>);

impl<P> Default for Widgets<P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<P: Principal> Projector for Widgets<P> {
    type Principal = P;
    type Key = Uuid;
    type Facts = ();
    type View = WidgetView;

    fn name(&self) -> ProjectorName {
        ProjectorName::from_static("widgets")
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a P,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move { Ok(Population::Keys(BTreeSet::new())) })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        _scope: LoadScope<'a, Uuid, P>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move { Ok(()) })
    }

    fn project(
        &self,
        _facts: &(),
        _key: &Uuid,
        _principal: &P,
    ) -> Result<Option<WidgetView>, EngineError> {
        Ok(None)
    }
}

crate::subscription_union! {
    generics [P: crate::principal::Principal + 'static] ;
    view = WidgetViews;
    delta = WidgetDelta { reset = WidgetReset, upsert = WidgetUpsert, remove = WidgetRemove };
    Widget => Widgets<P> => WidgetView,
}

fn encoded_widget() -> (Uuid, ErasedView) {
    let key = Uuid::now_v7();
    let erased = ErasedView::encode(&Widgets::<TestPrincipal>::default(), &key, &WidgetView { id: 7 })
        .expect("encode a widget view");
    (key, erased)
}

#[test]
fn the_generic_arm_maps_reset_upsert_and_remove_over_a_concrete_principal() {
    let (key, erased) = encoded_widget();

    let reset = Delta::Reset {
        views: vec![erased.clone()],
        revision: Revision::FIRST,
    };
    assert!(matches!(
        WidgetDelta::from_delta::<TestPrincipal>(&reset).expect("reset maps"),
        WidgetDelta::Reset(_)
    ));

    let upsert = Delta::Upsert {
        view: erased,
        revision: Revision::FIRST,
        cause: None,
    };
    assert!(matches!(
        WidgetDelta::from_delta::<TestPrincipal>(&upsert).expect("upsert maps"),
        WidgetDelta::Upsert(_)
    ));

    let remove = Delta::Remove {
        projector: ProjectorName::from_static("widgets"),
        key: KeyBytes::encode(&key).expect("encode key"),
        revision: Revision::FIRST,
        cause: None,
    };
    assert!(matches!(
        WidgetDelta::from_delta::<TestPrincipal>(&remove).expect("remove maps"),
        WidgetDelta::Remove(_)
    ));
}

#[test]
fn the_generic_arm_errs_naming_a_projector_the_union_does_not_map() {
    let erased = ErasedView::new(
        ProjectorName::from_static("unmapped"),
        KeyBytes::encode(&Uuid::now_v7()).expect("encode key"),
        ViewBytes::encode(&WidgetView { id: 1 }).expect("encode view"),
    );
    let reset = Delta::Reset {
        views: vec![erased],
        revision: Revision::FIRST,
    };
    match WidgetDelta::from_delta::<TestPrincipal>(&reset) {
        Ok(_) => panic!("a delta naming an unmapped projector must not map"),
        Err(err) => assert!(err.message.contains("unmapped")),
    }
}
