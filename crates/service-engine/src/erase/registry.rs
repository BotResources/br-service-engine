use futures_util::future::BoxFuture;

use crate::erase::context::Erase;
use crate::erase::manifest::Erased;
use crate::erase::person::{Erasable, PersonId};
use crate::error::EngineError;

pub(crate) trait ErasedErasable: Send + Sync + 'static {
    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EngineError>>;
}

pub(crate) struct ErasableAdapter<E>(E);

impl<E> ErasableAdapter<E> {
    pub(crate) fn new(erasable: E) -> Self {
        Self(erasable)
    }
}

impl<E: Erasable> ErasedErasable for ErasableAdapter<E> {
    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EngineError>> {
        Box::pin(async move {
            self.0
                .erase(cx, person)
                .await
                .map_err(|error| EngineError::Service(Box::new(error)))
        })
    }
}
