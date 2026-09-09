use chrono::TimeDelta;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Bulk, Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{Card, CardEvent, CardState};
use super::store::CardAggregate;
use super::view::CardsView;
use super::wire::CardDeadline;
use crate::kernel::{AppFault, AppPrincipal};

#[derive(Debug, Deserialize)]
pub struct AdvanceCard {
    pub id: Uuid,
}

impl MutationInput for AdvanceCard {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "advance_card";
}

pub fn advance_card<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: AdvanceCard,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let mut card = cx
            .load::<CardAggregate>(&input.id)
            .await?
            .ok_or(AppFault::NotFound)?;
        card.0.advance(cx.principal())?;
        let to = card.0.status;
        cx.save(&card).await?;
        cx.impact_caused::<Card, _>(&input.id, CardEvent::Advanced { to })?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct ScheduleCardDeadline {
    pub id: Uuid,
    pub in_seconds: i64,
}

impl MutationInput for ScheduleCardDeadline {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "schedule_card_deadline";
}

pub fn schedule_card_deadline<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: ScheduleCardDeadline,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let at = cx
            .now()
            .checked_add_signed(TimeDelta::seconds(input.in_seconds))
            .ok_or(AppFault::NotFound)?;
        cx.schedule_at(at, CardDeadline { card_id: input.id })?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct ImportCards {
    pub board_id: Uuid,
    pub titles: Vec<String>,
}

impl MutationInput for ImportCards {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "import_cards";
}

pub fn import_cards<'m>(
    cx: &'m mut Bulk<'m, AppPrincipal>,
    input: ImportCards,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        for title in input.titles {
            let card = CardAggregate(CardState::open(Uuid::now_v7(), input.board_id, title));
            cx.create(&card).await?;
        }
        cx.impact_all(CardsView);
        Ok(())
    })
}
