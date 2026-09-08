mod accumulator;
mod person;
mod projector;

use std::any::Any;

pub use accumulator::{AccumulatorAdapter, ErasedAccumulator, ErasedState, erase_accumulator};
pub use projector::{
    ErasedInverse, ErasedLoadScope, ErasedPopulation, ErasedProjector, ErasedWindowQuery,
    ProjectorAdapter, erase_projector,
};

pub use person::{Erasable, PersonId};

pub type ErasedFacts = Box<dyn Any + Send + Sync>;
