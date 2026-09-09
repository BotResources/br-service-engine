use chrono::TimeDelta;
use example_contract::CardReady;
use futures_util::future::BoxFuture;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::aggregate::{Card, CardState};
use super::store::CardAggregate;
use super::wire::{CardDeadline, InboundCreateCard, InboundPersonCreated, OutCardReady};
use crate::kernel::error::ReactionFault;

const DEADLINE: TimeDelta = TimeDelta::seconds(3600);

pub fn welcome_card_id(person: Uuid) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, person.as_bytes())
}

pub fn create_card<'r>(
    cx: &'r mut Reaction<'r>,
    msg: InboundCreateCard,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let cmd = msg.0;
        let card = CardAggregate(CardState::open(cmd.card_id, cmd.board_id, cmd.title));
        cx.create(&card).await?;
        cx.emit(OutCardReady(CardReady {
            card_id: cmd.card_id,
            board_id: cmd.board_id,
        }))?;
        let at = cx
            .now()
            .checked_add_signed(DEADLINE)
            .ok_or_else(|| ReactionFault::Terminal("deadline overflow".into()))?;
        cx.schedule_at(
            at,
            CardDeadline {
                card_id: cmd.card_id,
            },
        )?;
        cx.impact_caused::<Card, _>(&cmd.card_id, "created")?;
        Ok(())
    })
}

pub fn person_created<'r>(
    cx: &'r mut Reaction<'r>,
    msg: InboundPersonCreated,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let event = msg.0;
        let card_id = welcome_card_id(event.person_id);
        let card = CardAggregate(CardState::open(
            card_id,
            event.board_id,
            format!("Welcome {}", event.display_name),
        ));
        cx.create(&card).await?;
        cx.impact_caused::<Card, _>(&card_id, "welcomed")?;
        Ok(())
    })
}

pub fn card_deadline<'r>(
    cx: &'r mut Reaction<'r>,
    msg: CardDeadline,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let Some(mut card) = cx.load::<CardAggregate>(&msg.card_id).await? else {
            return Ok(());
        };
        card.0.pass_deadline();
        cx.save(&card).await?;
        cx.impact_caused::<Card, _>(&msg.card_id, "deadline")?;
        Ok(())
    })
}
