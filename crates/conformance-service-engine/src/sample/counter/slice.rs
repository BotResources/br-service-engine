use service_engine::persistence::{Aggregate, Persistence};
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::event::CounterEvent;

pub trait CounterAggregate: Aggregate
where
    <Self as Aggregate>::Store: Persistence<Key = Uuid, Event = CounterEvent>,
{
    type Noun: Noun<Key = Uuid>;

    fn open(key: Uuid, tenant: Uuid) -> Self;

    fn state(&self) -> &CounterState;

    fn state_mut(&mut self) -> &mut CounterState;
}
