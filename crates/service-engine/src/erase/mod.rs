mod context;
mod event;
mod gesture;
mod manifest;
mod person;
mod registry;

pub use context::Erase;
pub use gesture::{EraseOutcome, Eraser};
pub use manifest::Erased;
pub use person::{Erasable, PersonId};

pub(crate) use registry::{ErasableAdapter, ErasedErasable};
