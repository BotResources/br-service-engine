use service_engine::error::EngineError;
use service_engine::{Engine, ScopeManifest};

use crate::kernel::{AppPrincipal, scopes};
use crate::slices;

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(crate::kernel::principal::AppPrincipalResolver)?;
    engine.register_rls(crate::kernel::AppRls)?;

    #[cfg(feature = "board")]
    slices::board::register(engine)?;
    #[cfg(feature = "card")]
    slices::card::register(engine)?;
    #[cfg(feature = "ledger")]
    slices::ledger::register(engine)?;
    #[cfg(feature = "reply")]
    slices::reply::register(engine)?;
    #[cfg(feature = "roster")]
    slices::roster::register(engine)?;

    Ok(())
}

pub fn scopes() -> ScopeManifest {
    scopes::manifest()
}
