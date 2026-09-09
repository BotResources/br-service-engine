use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::Query;
use service_engine::graphql::SliceFragment;
use service_engine::session::{WindowParams, WindowSpec};
use uuid::Uuid;

use super::view::{RosterUsers, RosterView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = RosterViewUnion;
    delta = RosterDelta { reset = RosterReset, upsert = RosterUpsert, remove = RosterRemove };
    Person => RosterUsers => RosterView,
}

pub const FRAGMENT: SliceFragment = SliceFragment {
    slice: "roster",
    root_fields: &["person", "rosterDeltas"],
    types: &["RosterView"],
};

#[derive(Default)]
pub struct RosterQuery;

#[Object]
impl RosterQuery {
    async fn person(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<RosterView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch::<RosterUsers>(&id)
            .await
    }
}

#[derive(Default)]
pub struct RosterSubscription;

#[Subscription]
impl RosterSubscription {
    async fn roster_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<RosterDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(
                RosterUsers::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| RosterDelta::from_delta(&delta)))
    }
}
