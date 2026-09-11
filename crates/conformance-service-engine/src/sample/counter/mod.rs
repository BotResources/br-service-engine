pub mod crud;
pub mod domain;
pub mod event;
pub mod full;
pub mod full_erase;
pub mod full_view;
pub mod handler;
pub mod lockless;
pub mod slice;
pub mod soft;

pub use crud::{CrudCounter, CrudCounterNoun, CrudCounterStore};
pub use domain::{CounterError, CounterState, CounterView, view_of};
pub use event::{CounterEvent, EVENT_VERSION, upcast};
pub use full::{FullCounter, FullCounterNoun, FullCounterStore, replay_from_scratch};
pub use full_erase::erase_author;
pub use full_view::{FullCounterFacts, FullCounterProjector};
pub use handler::{
    BumpCrud, BumpFull, BumpFullCmd, BumpLockless, BumpSoft, CoarseCounterFault, CounterFault,
    OpenCrud, OpenFull, OpenLockless, OpenSoft, bump_crud, bump_full, bump_full_coords,
    bump_full_reaction, bump_lockless, bump_soft, open_crud, open_full, open_lockless, open_soft,
};
pub use lockless::{LocklessCounter, LocklessCounterNoun, LocklessCounterStore};
pub use slice::CounterAggregate;
pub use soft::{SoftCounter, SoftCounterNoun, SoftCounterStore};
