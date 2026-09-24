use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::error::EngineError;
use crate::impact::{Deps, ForeignKey, Impact};

use super::bind::{Bind, Column};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written {
    Inserted,
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

pub(super) fn rendered(key: &[Column]) -> Vec<String> {
    key.iter().map(|c| c.value.render()).collect()
}

pub(super) fn foreign_key_of(key: &[Column]) -> String {
    rendered(key).join("/")
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

pub(super) async fn upsert<R: KnownRow>(
    conn: &mut PgConnection,
    row: &R,
) -> Result<Written, EngineError> {
    let key = keyed::<R>(row.key())?;
    let values = row.values();
    let key_names: Vec<&'static str> = key.iter().map(|c| c.name).collect();
    let value_names: Vec<&'static str> = values.iter().map(|c| c.name).collect();

    let mut qb = QueryBuilder::<Postgres>::new("INSERT INTO ");
    qb.push(R::TABLE);
    qb.push(" (");
    {
        let mut cols = qb.separated(", ");
        for name in key_names.iter().chain(value_names.iter()) {
            cols.push(*name);
        }
    }
    qb.push(") VALUES (");
    let mut first = true;
    for column in key.into_iter().chain(values) {
        if !first {
            qb.push(", ");
        }
        first = false;
        column.value.push_bind_to(&mut qb);
    }
    qb.push(") ON CONFLICT (");
    {
        let mut cols = qb.separated(", ");
        for name in &key_names {
            cols.push(*name);
        }
    }
    qb.push(")");
    if value_names.is_empty() {
        qb.push(" DO NOTHING");
    } else {
        qb.push(" DO UPDATE SET ");
        {
            let mut sets = qb.separated(", ");
            for name in &value_names {
                sets.push(format!("{name} = EXCLUDED.{name}"));
            }
        }
        qb.push(" WHERE ");
        let mut first = true;
        for name in &value_names {
            if !first {
                qb.push(" OR ");
            }
            first = false;
            qb.push(format!(
                "{R}.{name} IS DISTINCT FROM EXCLUDED.{name}",
                R = R::TABLE
            ));
        }
    }
    qb.push(" RETURNING (xmax = 0) AS inserted");
    let outcome = qb.build().fetch_optional(conn).await?;
    Ok(match outcome {
        None => Written::Unchanged,
        Some(row) => {
            if row.try_get::<bool, _>("inserted")? {
                Written::Inserted
            } else {
                Written::Changed
            }
        }
    })
}

pub(super) async fn delete_by_key<R: KnownRow>(
    conn: &mut PgConnection,
    key: Vec<Column>,
) -> Result<bool, EngineError> {
    let key = keyed::<R>(key)?;
    let mut qb = QueryBuilder::<Postgres>::new("DELETE FROM ");
    qb.push(R::TABLE);
    qb.push(" WHERE ");
    let mut first = true;
    for column in key {
        if !first {
            qb.push(" AND ");
        }
        first = false;
        qb.push(column.name);
        qb.push(" = ");
        column.value.push_bind_to(&mut qb);
    }
    Ok(qb.build().execute(conn).await?.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
