use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EngineError};
use crate::name::ProjectorName;
use crate::projector::Projector;
use crate::wire::{Cause, KeyBytes, ViewBytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(u64);

impl Revision {
    pub const ZERO: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub const fn follows(self, previous: Self) -> bool {
        self.0 == previous.0 + 1
    }

    pub(crate) const fn rewound(self, steps: u64) -> Self {
        Self(self.0.saturating_sub(steps))
    }
}

impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasedView {
    pub projector: ProjectorName,
    pub key: KeyBytes,
    pub view: ViewBytes,
}

impl ErasedView {
    pub fn new(projector: ProjectorName, key: KeyBytes, view: ViewBytes) -> Self {
        Self {
            projector,
            key,
            view,
        }
    }

    pub fn encode<Pr: Projector>(
        projector: &Pr,
        key: &Pr::Key,
        view: &Pr::View,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            projector: projector.name(),
            key: KeyBytes::encode(key)?,
            view: ViewBytes::encode(view)?,
        })
    }

    pub fn decode<Pr: Projector>(&self) -> Result<(Pr::Key, Pr::View), DecodeError>
    where
        Pr::View: DeserializeOwned,
    {
        let key = serde_json::from_slice(self.key.as_slice()).map_err(DecodeError::Key)?;
        let view = serde_json::from_slice(self.view.as_slice()).map_err(DecodeError::View)?;
        Ok((key, view))
    }

    pub fn decode_from<Pr: Projector>(
        &self,
        projector: &Pr,
    ) -> Result<(Pr::Key, Pr::View), DecodeError>
    where
        Pr::View: DeserializeOwned,
    {
        let expected = projector.name();
        if expected != self.projector {
            return Err(DecodeError::Projector {
                expected,
                found: self.projector.clone(),
            });
        }
        self.decode::<Pr>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Delta {
    Reset {
        views: Vec<ErasedView>,
        revision: Revision,
    },
    Upsert {
        view: ErasedView,
        revision: Revision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<Cause>,
    },
    Remove {
        projector: ProjectorName,
        key: KeyBytes,
        revision: Revision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<Cause>,
    },
}

impl Delta {
    pub fn revision(&self) -> Revision {
        match self {
            Self::Reset { revision, .. }
            | Self::Upsert { revision, .. }
            | Self::Remove { revision, .. } => *revision,
        }
    }

    pub fn projector(&self) -> Option<&ProjectorName> {
        match self {
            Self::Reset { .. } => None,
            Self::Upsert { view, .. } => Some(&view.projector),
            Self::Remove { projector, .. } => Some(projector),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_revision_starts_at_one_and_advances_by_exactly_one() {
        assert_eq!(Revision::ZERO.get(), 0);
        assert_eq!(Revision::ZERO.next(), Revision::FIRST);
        assert_eq!(Revision::FIRST.get(), 1);
        assert!(Revision::FIRST.next().follows(Revision::FIRST));
        assert!(!Revision::FIRST.next().next().follows(Revision::FIRST));
    }
}
