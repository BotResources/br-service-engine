use super::*;
use crate::mirror::projection::Projection;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Typed {
    #[allow(dead_code)]
    v: u32,
}
impl Consumed for Typed {
    const PREFIX: &'static str = "typed/v1/";
}

struct NoProject;
impl Project<()> for NoProject {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        _cx: Projection<'a>,
        _keys: Vec<()>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }
}

fn ready<C: Consumed>() -> MirrorReady<(), NoProject> {
    Mirror::new(MirrorName::from_static("guarded"))
        .consume::<C>()
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject)
}

#[test]
fn a_raw_json_consumption_is_refused_without_the_escape_hatch() {
    let refusal = ready::<serde_json::Value>().validate().unwrap_err();
    assert!(matches!(
        refusal,
        EngineError::RawJsonConsumption {
            prefix: "loose/v1/",
            ..
        }
    ));
}

#[test]
fn a_typed_consumption_validates_and_carries_its_manifest() {
    let ready = ready::<Typed>();
    assert!(ready.validate().is_ok());
    let guard = &ready.guards()[0];
    assert_eq!(guard.prefix(), "typed/v1/");
    assert_eq!(guard.manifest(), Typed::manifest());
}

#[test]
fn a_required_key_outside_the_consumed_prefix_is_refused() {
    let refusal = Mirror::new(MirrorName::from_static("guarded"))
        .consume::<Typed>()
        .require_key::<Typed>("other/v1/needed")
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject)
        .validate()
        .unwrap_err();
    assert!(matches!(
        refusal,
        EngineError::RequiredKeyOutsidePrefix {
            prefix: "typed/v1/",
            key: "other/v1/needed",
            ..
        }
    ));
}

macro_rules! consumed_at {
    ($name:ident, $declared:literal) => {
        #[derive(Clone, Serialize, Deserialize)]
        struct $name {
            #[allow(dead_code)]
            v: u32,
        }
        impl Consumed for $name {
            const PREFIX: &'static str = $declared;
        }
    };
}

consumed_at!(DottedPrefix, "catalog.item.");
consumed_at!(SingleKey, "catalog.settings");
consumed_at!(SlashSingleKey, "catalog/_meta");
consumed_at!(CutMidSegment, "catalog.item_");
consumed_at!(EmptySegment, "catalog..");
consumed_at!(ManifestNamed, "catalog.item_manifest");

fn refusal_of<C: Consumed>() -> String {
    match ready::<C>().validate().unwrap_err() {
        EngineError::Config(message) => message,
        other => panic!("expected a configuration error, got {other:?}"),
    }
}

#[test]
fn a_dot_terminated_prefix_validates() {
    assert!(ready::<DottedPrefix>().validate().is_ok());
}

#[test]
fn a_single_key_validates_in_either_separator_grammar() {
    assert!(ready::<SingleKey>().validate().is_ok());
    assert!(ready::<SlashSingleKey>().validate().is_ok());
}

#[test]
fn every_accepted_form_joins_in_one_mirror() {
    let mirror = Mirror::new(MirrorName::from_static("guarded"))
        .consume::<Typed>()
        .consume::<DottedPrefix>()
        .consume::<SingleKey>()
        .consume::<SlashSingleKey>()
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject);
    assert!(mirror.validate().is_ok());
}

#[test]
fn a_consumption_cut_mid_segment_is_refused_naming_the_mirror_and_the_string() {
    let message = refusal_of::<CutMidSegment>();
    assert!(message.contains("mirror guarded consumes \"catalog.item_\""));
    assert!(message.contains("ends mid-segment"));
}

#[test]
fn a_consumption_with_an_empty_segment_is_refused() {
    assert!(refusal_of::<EmptySegment>().contains("empty segment"));
}

#[test]
fn a_single_key_named_like_a_manifest_is_refused() {
    assert!(refusal_of::<ManifestNamed>().contains("offer manifest"));
}

#[test]
fn a_required_key_may_be_the_consumed_single_key() {
    let mirror = Mirror::new(MirrorName::from_static("guarded"))
        .consume::<SingleKey>()
        .require_key::<SingleKey>("catalog.settings")
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject);
    assert!(mirror.validate().is_ok());
}

#[test]
fn a_required_key_under_a_dot_prefix_validates() {
    let mirror = Mirror::new(MirrorName::from_static("guarded"))
        .consume::<DottedPrefix>()
        .require_key::<DottedPrefix>("catalog.item.default")
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject);
    assert!(mirror.validate().is_ok());
}

#[test]
fn a_required_key_beneath_a_single_key_is_outside_what_the_mirror_consumes() {
    let refusal = Mirror::new(MirrorName::from_static("guarded"))
        .consume::<SingleKey>()
        .require_key::<SingleKey>("catalog.settings.extra")
        .keyed_by(|_shadows, _change| Vec::<()>::new())
        .project(NoProject)
        .validate()
        .unwrap_err();
    assert!(matches!(
        refusal,
        EngineError::RequiredKeyOutsidePrefix {
            prefix: "catalog.settings",
            key: "catalog.settings.extra",
            ..
        }
    ));
}
