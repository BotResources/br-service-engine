pub mod crud;
pub mod domain;
pub mod event;
pub mod full;
pub mod full_erase;
pub mod full_view;
pub mod handler;
pub mod slice;
pub mod soft;

pub use crud::{CrudCounter, CrudCounterNoun, CrudCounterStore};
pub use domain::{COUNTER_CEILING, CounterError, CounterState, CounterView, view_of};
pub use event::{CounterEvent, EVENT_VERSION, upcast};
pub use full::{FullCounter, FullCounterNoun, FullCounterStore, replay_from_scratch};
pub use full_erase::erase_author;
pub use full_view::{FullCounterFacts, FullCounterProjector};
pub use handler::{
    BumpCrud, BumpFull, BumpFullCmd, BumpSoft, CoarseCounterFault, CounterFault, OpenCrud,
    OpenFull, OpenSoft, bump_crud, bump_full, bump_full_coords, bump_full_reaction, bump_soft,
    open_crud, open_full, open_soft,
};
pub use slice::CounterAggregate;
pub use soft::{SoftCounter, SoftCounterNoun, SoftCounterStore};
