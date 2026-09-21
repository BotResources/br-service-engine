#[macro_export]
macro_rules! subscription_union {
    (
        generics [ $gen:ident : $($bound:tt)+ ] ;
        view = $view_union:ident;
        delta = $delta:ident { reset = $reset:ident, upsert = $upsert:ident, remove = $remove:ident };
        $( $variant:ident => $proj:ty => $view:ty ),+ $(,)?
    ) => {
        #[derive(::std::clone::Clone, ::async_graphql::Union)]
        pub enum $view_union {
            $( $variant($view), )+
        }

        impl $view_union {
            fn from_erased< $gen : $($bound)+ >(
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
            LanesPaused($crate::LanesPaused),
            LanesResumed($crate::LanesResumed),
        }

        impl $delta {
            pub fn from_delta< $gen : $($bound)+ >(
                delta: &$crate::Delta,
            ) -> ::core::result::Result<Self, ::async_graphql::Error> {
                match delta {
                    $crate::Delta::Reset { views, revision } => {
                        let mut projected = ::std::vec::Vec::with_capacity(views.len());
                        for view in views {
                            projected.push($view_union::from_erased::<$gen>(view)?);
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
                        view: $view_union::from_erased::<$gen>(view)?,
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

            pub fn from_lane_notice(notice: &$crate::LaneNotice) -> Self {
                match notice {
                    $crate::LaneNotice::Paused(lanes) => {
                        $delta::LanesPaused($crate::LanesPaused { lanes: lanes.clone() })
                    }
                    $crate::LaneNotice::Resumed(lanes) => {
                        $delta::LanesResumed($crate::LanesResumed { lanes: lanes.clone() })
                    }
                }
            }

            pub fn subscribe< $gen : $($bound)+ , D, N>(
                deltas: D,
                notices: N,
            ) -> impl ::futures_util::Stream<
                Item = ::core::result::Result<Self, ::async_graphql::Error>,
            >
            where
                D: ::futures_util::Stream<Item = $crate::Delta> + ::core::marker::Send + 'static,
                N: ::futures_util::Stream<Item = $crate::LaneNotice>
                    + ::core::marker::Send
                    + 'static,
            {
                use ::futures_util::StreamExt;
                let deltas = deltas.map(|delta| $delta::from_delta::<$gen>(&delta)).boxed();
                let notices = notices
                    .map(|notice| ::core::result::Result::Ok($delta::from_lane_notice(&notice)))
                    .boxed();
                ::futures_util::stream::select(deltas, notices)
            }
        }
    };

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
            LanesPaused($crate::LanesPaused),
            LanesResumed($crate::LanesResumed),
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

            pub fn from_lane_notice(notice: &$crate::LaneNotice) -> Self {
                match notice {
                    $crate::LaneNotice::Paused(lanes) => {
                        $delta::LanesPaused($crate::LanesPaused { lanes: lanes.clone() })
                    }
                    $crate::LaneNotice::Resumed(lanes) => {
                        $delta::LanesResumed($crate::LanesResumed { lanes: lanes.clone() })
                    }
                }
            }

            pub fn subscribe<D, N>(
                deltas: D,
                notices: N,
            ) -> impl ::futures_util::Stream<
                Item = ::core::result::Result<Self, ::async_graphql::Error>,
            >
            where
                D: ::futures_util::Stream<Item = $crate::Delta> + ::core::marker::Send + 'static,
                N: ::futures_util::Stream<Item = $crate::LaneNotice>
                    + ::core::marker::Send
                    + 'static,
            {
                use ::futures_util::StreamExt;
                let deltas = deltas.map(|delta| $delta::from_delta(&delta)).boxed();
                let notices = notices
                    .map(|notice| ::core::result::Result::Ok($delta::from_lane_notice(&notice)))
                    .boxed();
                ::futures_util::stream::select(deltas, notices)
            }
        }
    };
}
