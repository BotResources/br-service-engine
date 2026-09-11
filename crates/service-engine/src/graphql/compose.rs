#[macro_export]
macro_rules! compose_service {
    (
        principal = $p:ty ;
        $( slice $name:ident [ $feat:literal ] { $($body:tt)* } )+
    ) => {
        $( #[cfg(feature = $feat)] pub mod $name; )+

        $crate::compose_service!(@roots
            queries { }
            mutations { }
            subscriptions { }
            rest { $( [ $feat ] { $($body)* } )+ }
        );

        pub fn register(
            engine: &mut $crate::Engine<$p>,
        ) -> ::core::result::Result<(), $crate::error::EngineError> {
            $( #[cfg(feature = $feat)] { $name::register(engine)?; } )+
            ::core::result::Result::Ok(())
        }
    };

    (@roots
        queries { $($queries:tt)* }
        mutations { $($mutations:tt)* }
        subscriptions { $($subscriptions:tt)* }
        rest {
            [ $feat:literal ] {
                query = $qt:path , mutation = $mt:path , subscription = $st:path $(,)?
            }
            $($more:tt)*
        }
    ) => {
        $crate::compose_service!(@roots
            queries { $($queries)* #[cfg(feature = $feat)] $qt, }
            mutations { $($mutations)* #[cfg(feature = $feat)] $mt, }
            subscriptions { $($subscriptions)* #[cfg(feature = $feat)] $st, }
            rest { $($more)* }
        );
    };

    (@roots
        queries { $($queries:tt)* }
        mutations { $($mutations:tt)* }
        subscriptions { $($subscriptions:tt)* }
        rest {
            [ $feat:literal ] {
                query = $qt:path , subscription = $st:path $(,)?
            }
            $($more:tt)*
        }
    ) => {
        $crate::compose_service!(@roots
            queries { $($queries)* #[cfg(feature = $feat)] $qt, }
            mutations { $($mutations)* }
            subscriptions { $($subscriptions)* #[cfg(feature = $feat)] $st, }
            rest { $($more)* }
        );
    };

    (@roots
        queries { $($queries:tt)* }
        mutations { $($mutations:tt)* }
        subscriptions { $($subscriptions:tt)* }
        rest { }
    ) => {
        #[derive(::async_graphql::MergedObject, ::core::default::Default)]
        pub struct QueryRoot( $($queries)* );

        #[derive(::async_graphql::MergedObject, ::core::default::Default)]
        pub struct MutationRoot( $($mutations)* );

        #[derive(::async_graphql::MergedSubscription, ::core::default::Default)]
        pub struct SubscriptionRoot( $($subscriptions)* );
    };
}
