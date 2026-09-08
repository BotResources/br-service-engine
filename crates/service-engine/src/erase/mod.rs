mod accumulator;
mod context;
mod event;
mod gesture;
mod manifest;
mod person;
mod projector;
mod registry;

use std::any::Any;

pub use accumulator::{AccumulatorAdapter, ErasedAccumulator, ErasedState, erase_accumulator};
pub use projector::{
    ErasedInverse, ErasedLoadScope, ErasedPopulation, ErasedProjector, ErasedWindowQuery,
    ProjectorAdapter, erase_projector,
};

pub use context::Erase;
pub use gesture::{EraseOutcome, Eraser};
pub use manifest::Erased;
pub use person::{Erasable, PersonId};

pub(crate) use registry::{ErasableAdapter, ErasedErasable};

pub type ErasedFacts = Box<dyn Any + Send + Sync>;
