use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Bulk, Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{Card, CardState};
use super::store::CardAggregate;
use super::view::CardsView;
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
        cx.save(&card).await?;
        cx.impact_caused::<Card, _>(&input.id, "advanced")?;
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
