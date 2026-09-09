use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(crate::kernel::principal::AppPrincipalResolver)?;
    engine.register_rls(crate::kernel::AppRls)?;
    crate::slices::register(engine)?;
    Ok(())
}
