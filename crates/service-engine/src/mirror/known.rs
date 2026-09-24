use uuid::Uuid;

use crate::error::EngineError;
use crate::impact::{Deps, ForeignKey, Impact};

use super::bind::{Bind, Column};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written {
    Changed,
    Unchanged,
}

impl Written {
    pub fn is_effective(self) -> bool {
        !matches!(self, Written::Unchanged)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrincipalColumn {
    pub column: &'static str,
    pub deps: Deps,
}

pub trait KnownRow: Send + Sync + 'static {
    const TABLE: &'static str;
    const NAMESPACE: &'static str;
    const KEY: &'static [&'static str];
    const PRINCIPAL: Option<PrincipalColumn>;

    fn key(&self) -> Vec<Column>;
    fn values(&self) -> Vec<Column>;
}

pub(super) fn foreign_key_of(key: &[Column]) -> String {
    key.iter()
        .map(|c| c.value.render())
        .collect::<Vec<_>>()
        .join("/")
}

pub(super) fn keyed<R: KnownRow>(key: Vec<Column>) -> Result<Vec<Column>, EngineError> {
    let names: Vec<&'static str> = key.iter().map(|c| c.name).collect();
    if names != R::KEY {
        return Err(EngineError::Config(format!(
            "a {} key names the columns {names:?}, but the row declares KEY = {:?}",
            R::TABLE,
            R::KEY
        )));
    }
    if let Some(principal) = R::PRINCIPAL
        && !key
            .iter()
            .any(|c| c.name == principal.column && matches!(c.value, Bind::Uuid(_)))
    {
        return Err(EngineError::Config(format!(
            "{} declares the principal column {}, which must be a KEY column bound to a uuid",
            R::TABLE,
            principal.column
        )));
    }
    Ok(key)
}

pub(super) fn impacts_of<R: KnownRow>(key: &[String]) -> Result<Vec<Impact>, EngineError> {
    let mut impacts = vec![Impact::foreign(ForeignKey::new(
        R::NAMESPACE,
        &key.join("/"),
    )?)];
    if let Some(principal) = R::PRINCIPAL {
        let id = R::KEY
            .iter()
            .position(|name| *name == principal.column)
            .and_then(|at| key.get(at))
            .and_then(|text| Uuid::parse_str(text).ok())
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "{} declares the principal column {}, which must be a uuid KEY column",
                    R::TABLE,
                    principal.column
                ))
            })?;
        impacts.push(Impact::principal_facts(id.into(), principal.deps));
    }
    Ok(impacts)
}

#[cfg(test)]
mod tests;
