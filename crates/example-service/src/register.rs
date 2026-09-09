use service_engine::error::EngineError;
use service_engine::{Engine, ScopeManifest};

use crate::kernel::{AppPrincipal, scopes};

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(crate::kernel::principal::AppPrincipalResolver)?;
    engine.register_rls(crate::kernel::AppRls)?;
    crate::slices::register(engine)?;
    Ok(())
}

pub fn scopes() -> ScopeManifest {
    scopes::manifest()
}
