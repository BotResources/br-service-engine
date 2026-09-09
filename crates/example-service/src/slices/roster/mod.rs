pub mod graphql;
mod mirror;
mod view;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_projector(view::RosterUsers)?;
    engine.register_mirror(mirror::directory_mirror())?;
    engine.register_schema_slice(graphql::FRAGMENT)?;
    Ok(())
}
