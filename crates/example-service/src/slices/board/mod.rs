mod aggregate;
pub mod erase;
pub mod graphql;
mod mutations;
mod offer;
mod store;
mod view;

pub use store::boards_of;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_projector(view::BoardsView)?;
    engine.register_projector(view::OrgBoardsRls)?;
    engine.register_offer::<offer::BoardOffer>()?;
    engine.register_mutation::<mutations::CreateBoard, _>(mutations::create_board)?;
    engine.register_mutation::<mutations::ArchiveBoard, _>(mutations::archive_board)?;
    engine.register_mutation::<mutations::MintBoardInvite, _>(mutations::mint_board_invite)?;
    engine.register_erasable(erase::BoardEraser)?;
    engine.register_schema_slice(graphql::FRAGMENT)?;
    Ok(())
}
