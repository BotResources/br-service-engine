use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reason(&'static str);

impl Reason {
    pub const fn new(code: &'static str) -> Self {
        Self(code)
    }

    pub const fn code(&self) -> &'static str {
        self.0
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl Serialize for Reason {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Allowed,
    Blocked(Reason),
}

impl Gate {
    pub fn allowed() -> Self {
        Self::Allowed
    }

    pub fn blocked(reason: Reason) -> Self {
        Self::Blocked(reason)
    }

    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn reason(&self) -> Option<Reason> {
        match self {
            Self::Allowed => None,
            Self::Blocked(reason) => Some(*reason),
        }
    }

    pub fn require(self) -> Result<(), Reason> {
        match self {
            Self::Allowed => Ok(()),
            Self::Blocked(reason) => Err(reason),
        }
    }
}

impl Serialize for Gate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Allowed => {
                let mut gate = serializer.serialize_struct("Gate", 1)?;
                gate.serialize_field("allowed", &true)?;
                gate.end()
            }
            Self::Blocked(reason) => {
                let mut gate = serializer.serialize_struct("Gate", 2)?;
                gate.serialize_field("allowed", &false)?;
                gate.serialize_field("reason", reason)?;
                gate.end()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActionName(&'static str);

impl ActionName {
    pub const fn from_static(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for ActionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl Serialize for ActionName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Affordances(Vec<(ActionName, Gate)>);

impl Affordances {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn from_pairs(pairs: impl IntoIterator<Item = (ActionName, Gate)>) -> Self {
        let mut affordances = Self::new();
        for (action, gate) in pairs {
            affordances.insert(action, gate);
        }
        affordances
    }

    pub fn insert(&mut self, action: ActionName, gate: Gate) {
        match self.0.iter_mut().find(|(name, _)| *name == action) {
            Some(entry) => entry.1 = gate,
            None => self.0.push((action, gate)),
        }
    }

    pub fn with(mut self, action: ActionName, gate: Gate) -> Self {
        self.insert(action, gate);
        self
    }

    pub fn get(&self, action: ActionName) -> Option<Gate> {
        self.0
            .iter()
            .find(|(name, _)| *name == action)
            .map(|(_, gate)| *gate)
    }

    pub fn names(&self) -> impl Iterator<Item = ActionName> + '_ {
        self.0.iter().map(|(name, _)| *name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ActionName, Gate)> + '_ {
        self.0.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn require(&self, action: ActionName) -> Result<(), Reason> {
        match self.get(action) {
            Some(gate) => gate.require(),
            None => Err(Reason::new("unknown_action")),
        }
    }
}

impl Serialize for Affordances {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (action, gate) in &self.0 {
            map.serialize_entry(action.as_str(), gate)?;
        }
        map.end()
    }
}

pub trait Gated {
    type Principal;

    const ACTIONS: &'static [ActionName];

    fn gate(&self, action: ActionName, principal: &Self::Principal) -> Option<Gate>;

    fn affordances(&self, principal: &Self::Principal) -> Affordances;
}

mod check;
mod decode;

pub use check::{GateMismatch, check_gates_match_affordances};

#[macro_export]
macro_rules! gated {
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

#[cfg(test)]
mod tests;
