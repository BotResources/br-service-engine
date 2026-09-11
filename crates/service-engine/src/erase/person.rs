use futures_util::future::BoxFuture;
use uuid::Uuid;

use crate::erase::context::Erase;
use crate::erase::manifest::Erased;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersonId(pub Uuid);

impl PersonId {
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

pub trait Erasable: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, Self::Error>>;
}
