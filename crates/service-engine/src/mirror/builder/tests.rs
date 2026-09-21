
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
        _key: (),
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
