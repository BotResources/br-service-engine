#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reason(pub &'static str);

impl Reason {
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn reason(&self) -> Option<&Reason> {
        match self {
            Self::Allowed => None,
            Self::Blocked(reason) => Some(reason),
        }
    }

    pub fn require(self) -> Result<(), Reason> {
        match self {
            Self::Allowed => Ok(()),
            Self::Blocked(reason) => Err(reason),
        }
    }
}
