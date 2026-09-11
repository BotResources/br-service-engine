mod aggregate;
pub mod erase;
pub mod graphql;
mod mutations;
mod offer;
mod principal;
mod store;
#[cfg(test)]
mod tests;
mod view;

pub use principal::BoardMemberships;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub const BOARD_ARCHIVE: &str = "example:board_archive";

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.contribute_scopes(&[BOARD_ARCHIVE])?;
    engine.register_principal_fact(principal::load_board_memberships)?;
    engine.register_view(view::BoardsView)?;
    engine.register_view(view::OrgBoardsRls)?;
    engine.register_offer::<offer::BoardOffer>()?;
    engine.register_mutation::<mutations::CreateBoard, _>(mutations::create_board)?;
    engine.register_mutation::<mutations::ArchiveBoard, _>(mutations::archive_board)?;
    engine.register_mutation::<mutations::MintBoardInvite, _>(mutations::mint_board_invite)?;
    engine
        .register_mutation::<mutations::SetBoardMembership, _>(mutations::set_board_membership)?;
    engine.register_erasable(erase::BoardEraser)?;
    engine.register_schema_slice(graphql::FRAGMENT)?;
    Ok(())
}
