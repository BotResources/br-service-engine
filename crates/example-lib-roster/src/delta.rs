use crate::principal::RosterPrincipal;
use crate::view::{RosterUsers, RosterView};

service_engine::subscription_union! {
    generics [ P: RosterPrincipal ];
    view = RosterViewUnion;
    delta = RosterDelta { reset = RosterReset, upsert = RosterUpsert, remove = RosterRemove };
    Person => service_engine::ViewProjector<RosterUsers<P>> => RosterView,
}
