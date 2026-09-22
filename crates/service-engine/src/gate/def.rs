#[macro_export]
macro_rules! gated {
    (
        generics [ $($gen:tt)* ] ;
        $aggregate:ty, $principal:ty ;
        $( $action:literal => fn $method:ident ( $this:ident, $binder:pat_param ) $body:block )+
    ) => {
        impl< $($gen)* > $aggregate {
            $(
                pub fn $method(&self, $binder: &$principal) -> $crate::gate::Gate {
                    let $this = self;
                    $body
                }
            )+
        }

        impl< $($gen)* > $crate::gate::Gated for $aggregate {
            type Principal = $principal;

            const ACTIONS: &'static [$crate::gate::ActionName] =
                &[ $( $crate::gate::ActionName::from_static($action) ),+ ];

            fn gate(
                &self,
                action: $crate::gate::ActionName,
                principal: &$principal,
            ) -> ::core::option::Option<$crate::gate::Gate> {
                match action.as_str() {
                    $( $action => ::core::option::Option::Some(self.$method(principal)), )+
                    _ => ::core::option::Option::None,
                }
            }

            fn affordances(&self, principal: &$principal) -> $crate::gate::Affordances {
                $crate::gate::Affordances::from_pairs([
                    $(
                        (
                            $crate::gate::ActionName::from_static($action),
                            self.$method(principal),
                        )
                    ),+
                ])
            }
        }
    };

    (
        $aggregate:ty, $principal:ty ;
        $( $action:literal => fn $method:ident ( $this:ident, $binder:pat_param ) $body:block )+
    ) => {
        impl $aggregate {
            $(
                pub fn $method(&self, $binder: &$principal) -> $crate::gate::Gate {
                    let $this = self;
                    $body
                }
            )+
        }

        impl $crate::gate::Gated for $aggregate {
            type Principal = $principal;

            const ACTIONS: &'static [$crate::gate::ActionName] =
                &[ $( $crate::gate::ActionName::from_static($action) ),+ ];

            fn gate(
                &self,
                action: $crate::gate::ActionName,
                principal: &$principal,
            ) -> ::core::option::Option<$crate::gate::Gate> {
                match action.as_str() {
                    $( $action => ::core::option::Option::Some(self.$method(principal)), )+
                    _ => ::core::option::Option::None,
                }
            }

            fn affordances(&self, principal: &$principal) -> $crate::gate::Affordances {
                $crate::gate::Affordances::from_pairs([
                    $(
                        (
                            $crate::gate::ActionName::from_static($action),
                            self.$method(principal),
                        )
                    ),+
                ])
            }
        }
    };
}
