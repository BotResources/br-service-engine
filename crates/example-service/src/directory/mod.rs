pub mod extension;
mod mirror;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

impl example_lib_roster::RosterPrincipal for AppPrincipal {}

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_mirror(mirror::directory_mirror())?;
    Ok(())
}
