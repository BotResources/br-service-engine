mod aggregate;
mod cron;
pub mod graphql;
mod mutations;
mod reactions;
mod store;
#[cfg(test)]
mod tests;
mod view;
mod wire;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub const CARD_ADVANCE: &str = "example:card_advance";

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.contribute_scopes(&[CARD_ADVANCE])?;
    engine.register_view(view::CardsView)?;
    engine.register_mutation::<mutations::AdvanceCard, _>(mutations::advance_card)?;
    engine.register_mutation::<mutations::ScheduleCardDeadline, _>(
        mutations::schedule_card_deadline,
    )?;
    engine.register_bulk::<mutations::ImportCards, _>(mutations::import_cards)?;
    engine.register_reaction::<wire::InboundCreateCard, _, _>(
        "card-create",
        reactions::create_card,
    )?;
    engine.register_reaction::<wire::InboundPersonCreated, _, _>(
        "card-person-created",
        reactions::person_created,
    )?;
    engine
        .register_reaction::<wire::CardDeadline, _, _>("card-deadline", reactions::card_deadline)?;
    engine.register_cron(cron::PurgeDoneCards)?;
    engine.register_schema_slice(
        service_engine::graphql::SliceFragment::derive::<
            graphql::CardItemQuery,
            graphql::CardItemMutation,
            async_graphql::EmptySubscription,
        >("card"),
    )?;
    engine.register_schema_slice(
        service_engine::graphql::SliceFragment::derive::<
            graphql::CardBoardQuery,
            graphql::CardBoardMutation,
            graphql::CardBoardSubscription,
        >("card"),
    )?;
    Ok(())
}
