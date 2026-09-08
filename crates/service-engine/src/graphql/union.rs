use async_graphql::{Error, Json};
use serde::de::DeserializeOwned;

use crate::delta::ErasedView;
use crate::gate::Affordances;
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
    Ok(Some(erased.view.decode::<Pr::View>().map_err(Error::from)?))
}

pub fn cause_json(cause: Option<&Cause>) -> Result<Option<JsonScalar>, Error> {
    match cause {
        Some(cause) => Ok(Some(Json(cause.decode::<serde_json::Value>()?))),
        None => Ok(None),
    }
}

pub fn key_json(key: &KeyBytes) -> Result<JsonScalar, Error> {
    Ok(Json(key.decode::<serde_json::Value>()?))
}

#[macro_export]
macro_rules! subscription_union {
    (
        view = $view_union:ident;
        delta = $delta:ident { reset = $reset:ident, upsert = $upsert:ident, remove = $remove:ident };
        $( $variant:ident => $proj:ty => $view:ty ),+ $(,)?
    ) => {
        #[derive(::std::clone::Clone, ::async_graphql::Union)]
        pub enum $view_union {
            $( $variant($view), )+
        }

        impl $view_union {
            fn from_erased(
                erased: &$crate::delta::ErasedView,
            ) -> ::core::result::Result<Self, ::async_graphql::Error> {
                $(
                    if let ::core::option::Option::Some(view) =
                        $crate::graphql::typed_view::<$proj>(erased)?
                    {
                        return ::core::result::Result::Ok(Self::$variant(view));
                    }
                )+
                ::core::result::Result::Err(::async_graphql::Error::new(::std::format!(
                    "the delta names projector `{}`, which this subscription union does not map",
                    erased.projector
                )))
            }
        }

        #[derive(::std::clone::Clone, ::async_graphql::SimpleObject)]
        pub struct $reset {
            pub revision: u64,
            pub views: ::std::vec::Vec<$view_union>,
        }

        #[derive(::std::clone::Clone, ::async_graphql::SimpleObject)]
        pub struct $upsert {
            pub revision: u64,
            pub view: $view_union,
            pub cause: ::core::option::Option<$crate::graphql::JsonScalar>,
        }

        #[derive(::std::clone::Clone, ::async_graphql::SimpleObject)]
        pub struct $remove {
            pub revision: u64,
            pub projector: ::std::string::String,
            pub key: $crate::graphql::JsonScalar,
            pub cause: ::core::option::Option<$crate::graphql::JsonScalar>,
        }

        #[derive(::std::clone::Clone, ::async_graphql::Union)]
        pub enum $delta {
            Reset($reset),
            Upsert($upsert),
            Remove($remove),
        }

        impl $delta {
            pub fn from_delta(
                delta: &$crate::Delta,
            ) -> ::core::result::Result<Self, ::async_graphql::Error> {
                match delta {
                    $crate::Delta::Reset { views, revision } => {
                        let mut projected = ::std::vec::Vec::with_capacity(views.len());
                        for view in views {
                            projected.push($view_union::from_erased(view)?);
                        }
                        ::core::result::Result::Ok($delta::Reset($reset {
                            revision: revision.get(),
                            views: projected,
                        }))
                    }
                    $crate::Delta::Upsert {
                        view,
                        revision,
                        cause,
                    } => ::core::result::Result::Ok($delta::Upsert($upsert {
                        revision: revision.get(),
                        view: $view_union::from_erased(view)?,
                        cause: $crate::graphql::cause_json(cause.as_ref())?,
                    })),
                    $crate::Delta::Remove {
                        projector,
                        key,
                        revision,
                        cause,
                    } => ::core::result::Result::Ok($delta::Remove($remove {
                        revision: revision.get(),
                        projector: projector.to_string(),
                        key: $crate::graphql::key_json(key)?,
                        cause: $crate::graphql::cause_json(cause.as_ref())?,
                    })),
                }
            }
        }
    };
}
