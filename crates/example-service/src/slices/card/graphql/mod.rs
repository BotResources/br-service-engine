//! The card slice's GraphQL surface, composed of two capability fragments over
//! the one `Card` aggregate:
//!
//! - [`item`] — reading and driving a single card (`card`, `advanceCard`,
//!   `scheduleCardDeadline`);
//! - [`board`] — the board-scoped view of cards (`cards`, `importCards`,
//!   `pageCards`, the delta subscriptions).
//!
//! Each capability is registered as its own [`SliceFragment`] (see the slice's
//! `register`), and the engine reads each one's root fields and owned object
//! types from its `#[Object]` impls. Both fragments name the same aggregate
//! (`"card"`), so they legitimately share `CardView`; the merged roots below
//! are what `compose_service!` folds into the service's top-level roots.

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
