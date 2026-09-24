#[macro_export]
macro_rules! roster_slice {
    (prefix = $prefix:ident ; principal = $p:ty) => {
        ::service_engine::pastey::paste! {
            #[derive(::core::default::Default)]
            pub struct RosterQuery;

            #[::async_graphql::Object]
            impl RosterQuery {
                async fn [<$prefix _person>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                    id: ::uuid::Uuid,
                ) -> ::async_graphql::Result<::core::option::Option<$crate::RosterView>> {
                    ::service_engine::Query::<$p>::new(ctx)?
                        .fetch_view::<$crate::RosterUsers<$p>>(&id)
                        .await
                }
            }

            #[derive(::core::default::Default)]
            pub struct RosterSubscription;

            #[::async_graphql::Subscription]
            impl RosterSubscription {
                async fn [<$prefix _roster_deltas>](
                    &self,
                    ctx: &::async_graphql::Context<'_>,
                ) -> ::async_graphql::Result<
                    impl ::futures_util::Stream<
                        Item = ::async_graphql::Result<$crate::RosterDelta>,
                    >,
                > {
                    use ::futures_util::StreamExt;
                    let stream = ::service_engine::attach::<$p>(
                        ctx,
                        ::std::vec![::service_engine::session::WindowSpec::new(
                            $crate::RosterUsers::<$p>::NAME,
                            ::service_engine::session::WindowParams::none(),
                            false,
                        )],
                    )
                    .await?;
                    ::core::result::Result::Ok(
                        stream.map(|delta| $crate::RosterDelta::from_delta::<$p>(&delta)),
                    )
                }
            }

            pub fn register(
                engine: &mut ::service_engine::Engine<$p>,
            ) -> ::core::result::Result<(), ::service_engine::error::EngineError> {
                engine.register_view($crate::RosterUsers::<$p>::default())?;
                engine.register_schema_slice(
                    ::service_engine::graphql::SliceFragment::derive::<
                        RosterQuery,
                        ::async_graphql::EmptyMutation,
                        RosterSubscription,
                    >("roster"),
                )?;
                ::core::result::Result::Ok(())
            }
        }
    };
}
