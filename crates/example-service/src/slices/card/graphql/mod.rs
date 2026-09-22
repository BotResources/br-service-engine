mod board;
mod item;

use async_graphql::{MergedObject, MergedSubscription};

pub use board::{CardBoardMutation, CardBoardQuery, CardBoardSubscription, CardDelta, CardPage};
pub use item::{CardItemMutation, CardItemQuery};

#[derive(MergedObject, Default)]
pub struct CardQuery(CardItemQuery, CardBoardQuery);

#[derive(MergedObject, Default)]
pub struct CardMutation(CardItemMutation, CardBoardMutation);

#[derive(MergedSubscription, Default)]
pub struct CardSubscription(CardBoardSubscription);
