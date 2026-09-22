#[macro_export]
macro_rules! compose_service {
    (
        principal = $p:ty ;
        prefix = $prefix:ident ;
        $( slice $name:ident [ $feat:literal ] $(from $($lib:ident)::+)? { $($body:tt)* } )+
    ) => {
        $(
            $crate::compose_service!(
                @slice_mod $name [ $feat ] $prefix $p ; $(from $($lib)::+)?
            );
        )+

        $crate::compose_service!(@roots
            queries { }
            mutations { }
            subscriptions { }
            rest { $( [ $feat ] { $($body)* } )+ }
        );

        pub const ROOT_PREFIX_SNAKE: &str = ::core::stringify!($prefix);

        pub fn register(
            engine: &mut $crate::Engine<$p>,
        ) -> ::core::result::Result<(), $crate::error::EngineError> {
            engine.declare_root_prefix($crate::graphql::RootPrefix::from_snake(ROOT_PREFIX_SNAKE)?)?;
            $( #[cfg(feature = $feat)] { $name::register(engine)?; } )+
            ::core::result::Result::Ok(())
        }
    };

    (@slice_mod $name:ident [ $feat:literal ] $prefix:ident $p:ty ; from $($lib:ident)::+ ) => {
        #[cfg(feature = $feat)]
        pub mod $name {
            $($lib)::+ ! (prefix = $prefix ; principal = $p);
        }
    };

    (@slice_mod $name:ident [ $feat:literal ] $prefix:ident $p:ty ; ) => {
        #[cfg(feature = $feat)]
        pub mod $name;
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
