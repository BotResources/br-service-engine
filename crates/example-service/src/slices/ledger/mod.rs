mod aggregate;
pub mod erase;
pub mod graphql;
mod mutations;
mod store;
mod view;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_projector(view::LedgersView)?;
    engine.register_mutation::<mutations::RecordEntry, _>(mutations::record_entry)?;
    engine.register_erasable(erase::LedgerEraser)?;
    engine.register_schema_slice(graphql::FRAGMENT)?;
    Ok(())
}
