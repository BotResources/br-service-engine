use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct CutMidSegment {
    #[allow(dead_code)]
    v: u32,
}
impl Consumed for CutMidSegment {
    const PREFIX: &'static str = "catalog/item_";
}

#[derive(Clone, Serialize, Deserialize)]
struct Single {
    #[allow(dead_code)]
    v: u32,
}
impl Consumed for Single {
    const PREFIX: &'static str = "catalog.settings";
}

#[derive(Clone, Serialize, Deserialize)]
struct Versioned {
    v: u32,
}
impl Consumed for Versioned {
    const PREFIX: &'static str = "catalog.versioned.";
    const VERSION: u16 = 2;
    fn wire_version(value: &Self) -> Option<u16> {
        u16::try_from(value.v).ok()
    }
}

fn versioned_key() -> KvKey {
    KvKey::new("catalog.versioned.one").unwrap()
}

fn reason_and_retire(effect: Effect) -> (String, Applier) {
    match effect {
        Effect::Rejected { reason, retire } => (reason, retire),
        _ => panic!("the value was expected to be rejected"),
    }
}

#[test]
fn a_value_that_does_not_decode_is_rejected_with_a_code_and_never_applied() {
    for bytes in [&b"not json"[..], &b"{\"w\": 1}"[..], &b"[1, 2"[..]] {
        let slot = Slot::<Versioned>::from_json(bytes);
        let effect = slot_effect::<Versioned>(versioned_key(), slot, 9);
        let (reason, _) = reason_and_retire(effect);
        assert!(reason.starts_with("undecodable_value"), "{reason}");
    }
}

#[test]
fn an_undecodable_overwrite_retires_the_good_value_it_replaces() {
    let mut shadows = Shadows::new();
    shadows.put_at::<Versioned>(versioned_key(), Versioned { v: 2 }, 4);
    let slot = Slot::<Versioned>::from_json(b"garbage");
    let (_, retire) = reason_and_retire(slot_effect::<Versioned>(versioned_key(), slot, 5));
    retire(&mut shadows);
    assert!(
        shadows
            .shadow::<Versioned>()
            .get(&versioned_key())
            .is_none()
    );
}

#[test]
fn the_wrong_wire_version_and_the_undecodable_value_carry_different_reasons() {
    let wrong = Slot::Value(Versioned { v: 3 });
    let (wire, _) = reason_and_retire(slot_effect::<Versioned>(versioned_key(), wrong, 1));
    let bad = Slot::<Versioned>::from_json(b"x");
    let (undecodable, _) = reason_and_retire(slot_effect::<Versioned>(versioned_key(), bad, 2));
    assert!(wire.contains("wire version 3"), "{wire}");
    assert!(
        undecodable.starts_with("undecodable_value"),
        "{undecodable}"
    );
    assert_ne!(wire, undecodable);
}

#[test]
fn a_good_value_at_the_right_version_is_applied() {
    let good = Slot::Value(Versioned { v: 2 });
    let effect = slot_effect::<Versioned>(versioned_key(), good, 1);
    let Effect::Apply(apply) = effect else {
        panic!("a good value was expected to apply");
    };
    let mut shadows = Shadows::new();
    apply(&mut shadows);
    assert!(
        shadows
            .shadow::<Versioned>()
            .get(&versioned_key())
            .is_some()
    );
}

#[derive(Clone, Serialize, Deserialize)]
struct Dotted {
    #[allow(dead_code)]
    v: u32,
}
impl Consumed for Dotted {
    const PREFIX: &'static str = "catalog.item.";
}

#[test]
fn a_consumption_cut_mid_segment_defers_the_error_instead_of_panicking() {
    let consumption = Consumption::of::<CutMidSegment>();
    assert_eq!(consumption.scope, Err(ScopeRefusal::MidSegment('_')));
}

#[test]
fn a_single_key_consumption_carries_no_manifest() {
    let consumption = Consumption::of::<Single>();
    let scope = consumption.scope.expect("a whole key is accepted");
    assert_eq!(scope.manifest(), None);
}

#[test]
fn a_dot_prefix_consumption_carries_its_sibling_manifest() {
    let consumption = Consumption::of::<Dotted>();
    let scope = consumption
        .scope
        .expect("a dot-terminated prefix is accepted");
    assert_eq!(
        scope.manifest().map(KvKey::as_str),
        Some("catalog.item_manifest")
    );
}
