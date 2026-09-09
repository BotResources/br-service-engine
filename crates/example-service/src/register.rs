use service_engine::error::EngineError;
use service_engine::{Engine, ScopeManifest};

use crate::kernel::{AppPrincipal, scopes};

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(crate::kernel::principal::AppPrincipalResolver)?;
    engine.register_rls(crate::kernel::AppRls)?;

    #[cfg(feature = "board")]
    crate::slices::board::register(engine)?;
    #[cfg(feature = "card")]
    crate::slices::card::register(engine)?;
    #[cfg(feature = "ledger")]
    crate::slices::ledger::register(engine)?;
    #[cfg(feature = "reply")]
    crate::slices::reply::register(engine)?;
    #[cfg(feature = "roster")]
    crate::slices::roster::register(engine)?;

    Ok(())
}

pub fn scopes() -> ScopeManifest {
    scopes::manifest()
}
