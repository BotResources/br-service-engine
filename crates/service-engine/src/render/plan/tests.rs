use super::coalesced_cause;
use crate::impact::{Dims, Impact};
use crate::name::NounName;
use crate::wire::{Cause, Noun};

struct Assignment;

impl Noun for Assignment {
    type Key = uuid::Uuid;
    const NAME: NounName = NounName::from_static("assignment");
}

fn caused(cause: &str) -> Impact {
    Impact::resource_caused::<Assignment>(
        &uuid::Uuid::now_v7(),
        Dims::ALL,
        Cause::encode(&cause).expect("a cause encodes"),
    )
    .expect("a key encodes")
}

fn uncaused() -> Impact {
    Impact::resource::<Assignment>(&uuid::Uuid::now_v7(), Dims::ALL).expect("a key encodes")
}

fn decode(cause: &Cause) -> String {
    cause.decode::<String>().expect("the cause round-trips")
}

#[test]
fn a_coalesced_delta_carries_the_cause_of_the_last_impact_it_folds() {
    let impacts = vec![caused("created"), caused("renamed")];
    let indices = vec![0, 1];
    let cause = coalesced_cause(Some(&indices), &impacts).expect("a cause survives the fold");
    assert_eq!(decode(&cause), "renamed");
}

#[test]
fn the_last_impact_that_carries_a_cause_wins_even_when_a_later_one_is_causeless() {
    // create (cause) -> touch (no cause): the create's cause still surfaces,
    // because the causeless later impact contributes nothing to attribute.
    let impacts = vec![caused("created"), uncaused()];
    let indices = vec![0, 1];
    let cause = coalesced_cause(Some(&indices), &impacts).expect("the earlier cause survives");
    assert_eq!(decode(&cause), "created");
}

#[test]
fn a_causeless_impact_between_two_caused_ones_does_not_shadow_the_last_cause() {
    let impacts = vec![caused("created"), uncaused(), caused("renamed")];
    let indices = vec![0, 1, 2];
    let cause = coalesced_cause(Some(&indices), &impacts).expect("a cause survives the fold");
    assert_eq!(decode(&cause), "renamed");
}

#[test]
fn last_is_defined_by_frame_arrival_order_not_the_order_indices_were_accumulated() {
    // The touching indices for one key can be gathered across windows out of
    // frame order; the cause is still the highest index that carries one.
    let impacts = vec![caused("created"), caused("renamed")];
    let indices = vec![1, 0];
    let cause = coalesced_cause(Some(&indices), &impacts).expect("a cause survives the fold");
    assert_eq!(decode(&cause), "renamed");
}

#[test]
fn a_fold_of_only_causeless_impacts_carries_no_cause() {
    let impacts = vec![uncaused(), uncaused()];
    let indices = vec![0, 1];
    assert!(coalesced_cause(Some(&indices), &impacts).is_none());
}

#[test]
fn a_vanished_key_with_no_touching_impact_carries_no_cause() {
    let impacts = vec![caused("created")];
    assert!(coalesced_cause(None, &impacts).is_none());
}
