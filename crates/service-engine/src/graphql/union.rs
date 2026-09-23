use async_graphql::{Error, Json};
use serde::de::DeserializeOwned;

use crate::delta::ErasedView;
use crate::gate::Affordances;
use crate::graphql::error::OrInternal;
use crate::projector::Projector;
use crate::wire::{Cause, KeyBytes};

pub type JsonScalar = Json<serde_json::Value>;

async_graphql::scalar!(Affordances);

pub fn typed_view<Pr>(erased: &ErasedView) -> Result<Option<Pr::View>, Error>
where
    Pr: Projector + Default,
    Pr::View: DeserializeOwned,
{
    if erased.projector != Pr::default().name() {
        return Ok(None);
    }
    Ok(Some(
        erased
            .view
            .decode::<Pr::View>()
            .or_internal("decode a subscription view")?,
    ))
}

pub fn typed_presence_view<Pr>(erased: &ErasedView) -> Result<Option<Pr::View>, Error>
where
    Pr: crate::presence::Presence,
    Pr::View: DeserializeOwned,
{
    if erased.projector != Pr::NAME {
        return Ok(None);
    }
    Ok(Some(
        erased
            .view
            .decode::<Pr::View>()
            .or_internal("decode a subscription view")?,
    ))
}

pub fn cause_json(cause: Option<&Cause>) -> Result<Option<JsonScalar>, Error> {
    match cause {
        Some(cause) => Ok(Some(Json(
            cause
                .decode::<serde_json::Value>()
                .or_internal("decode a delta cause")?,
        ))),
        None => Ok(None),
    }
}

pub fn key_json(key: &KeyBytes) -> Result<JsonScalar, Error> {
    Ok(Json(
        key.decode::<serde_json::Value>()
            .or_internal("decode a delta key")?,
    ))
}

mod presence;
mod subscription;

#[cfg(test)]
mod tests;
