#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeManifest {
    scopes: Vec<&'static str>,
}

impl ScopeManifest {
    pub fn of(groups: &[&[&'static str]]) -> Self {
        Self {
            scopes: groups
                .iter()
                .flat_map(|group| group.iter().copied())
                .collect(),
        }
    }

    pub fn scopes(&self) -> &[&'static str] {
        &self.scopes
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }
}
