use std::collections::{BTreeMap, BTreeSet};

use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use crate::error::EngineError;

use super::bind::{Bind, Column};
use super::known::{KnownRow, foreign_key_of, keyed};

const BINDS_PER_STATEMENT: usize = 30_000;

type Fingerprint = Vec<(&'static str, (u8, String))>;

type TypedKey = Vec<(u8, String)>;

struct Incoming {
    key: Vec<Column>,
    values: Vec<Column>,
}

pub(super) async fn replace_rows<R: KnownRow>(
    conn: &mut PgConnection,
    scope: &[Column],
    rows: Vec<R>,
) -> Result<BTreeSet<Vec<String>>, EngineError> {
    let rows = admit::<R>(scope, rows)?;
    let mut changed = BTreeSet::new();
    let per_row = rows
        .first()
        .map_or(1, |row| row.key.len() + row.values.len());
    let kept: Vec<Vec<String>> = (0..R::KEY.len())
        .map(|at| rows.iter().map(|row| row.key[at].value.render()).collect())
        .collect();
    let mut rows = rows.into_iter().peekable();
    while rows.peek().is_some() {
        let chunk: Vec<Incoming> = rows
            .by_ref()
            .take((BINDS_PER_STATEMENT / per_row).max(1))
            .collect();
        let mut upsert = upsert_chunk::<R>(chunk);
        let written = upsert.build().fetch_all(&mut *conn).await?;
        changed.extend(returned_keys(&written)?);
    }
    let mut retire = retire_stale::<R>(scope, &kept);
    let retired = retire.build().fetch_all(&mut *conn).await?;
    changed.extend(returned_keys(&retired)?);
    Ok(changed)
}

fn admit<R: KnownRow>(scope: &[Column], rows: Vec<R>) -> Result<Vec<Incoming>, EngineError> {
    if let Some(column) = scope.iter().find(|c| matches!(c.value, Bind::Null)) {
        return Err(refusal::<R>(format!(
            "scope column {} is null, so it would match no row",
            column.name
        )));
    }
    let mut admitted: BTreeMap<TypedKey, (Fingerprint, Incoming)> = BTreeMap::new();
    let mut value_names: Option<Vec<&'static str>> = None;
    for row in rows {
        let key = keyed::<R>(row.key())?;
        if let Some(column) = key.iter().find(|c| !c.value.renders_as_its_sql_text()) {
            return Err(refusal::<R>(format!(
                "key column {} must be a uuid, text, integer or boolean",
                column.name
            )));
        }
        let values = row.values();
        let names: Vec<&'static str> = values.iter().map(|c| c.name).collect();
        match &value_names {
            Some(first) if *first != names => {
                return Err(refusal::<R>(format!(
                    "rows name different value columns: {first:?} and {names:?}"
                )));
            }
            Some(_) => {}
            None => value_names = Some(names),
        }
        if let Some(outside) = scope.iter().find(|s| !carries(&key, &values, s)) {
            return Err(refusal::<R>(format!(
                "row {} lies outside the replaced scope on column {}",
                foreign_key_of(&key),
                outside.name
            )));
        }
        let typed_key: TypedKey = key.iter().map(|c| c.value.fingerprint()).collect();
        let fingerprint = values
            .iter()
            .map(|c| (c.name, c.value.fingerprint()))
            .collect();
        match admitted.get(&typed_key) {
            Some((held, _)) if *held == fingerprint => {}
            Some(_) => {
                return Err(refusal::<R>(format!(
                    "two rows share the key {} with different values",
                    foreign_key_of(&key)
                )));
            }
            None => {
                admitted.insert(typed_key, (fingerprint, Incoming { key, values }));
            }
        }
    }
    Ok(admitted.into_values().map(|(_, row)| row).collect())
}

fn carries(key: &[Column], values: &[Column], scope: &Column) -> bool {
    key.iter()
        .chain(values)
        .any(|c| c.name == scope.name && c.value.fingerprint() == scope.value.fingerprint())
}

fn refusal<R: KnownRow>(detail: String) -> EngineError {
    EngineError::Config(format!("replace_rows on {}: {detail}", R::TABLE))
}

fn upsert_chunk<R: KnownRow>(chunk: Vec<Incoming>) -> QueryBuilder<'static, Postgres> {
    let value_names: Vec<&'static str> = chunk
        .first()
        .map(|row| row.values.iter().map(|c| c.name).collect())
        .unwrap_or_default();
    let mut qb = QueryBuilder::<Postgres>::new("INSERT INTO ");
    qb.push(R::TABLE);
    qb.push(" (");
    qb.push(
        R::KEY
            .iter()
            .chain(value_names.iter())
            .copied()
            .collect::<Vec<_>>()
            .join(", "),
    );
    qb.push(") VALUES ");
    for (at, row) in chunk.into_iter().enumerate() {
        qb.push(if at == 0 { "(" } else { ", (" });
        for (column_at, column) in row.key.into_iter().chain(row.values).enumerate() {
            if column_at > 0 {
                qb.push(", ");
            }
            column.value.push_bind_to(&mut qb);
        }
        qb.push(")");
    }
    qb.push(" ON CONFLICT (");
    qb.push(R::KEY.join(", "));
    qb.push(")");
    if value_names.is_empty() {
        qb.push(" DO NOTHING");
    } else {
        qb.push(" DO UPDATE SET ");
        qb.push(
            value_names
                .iter()
                .map(|name| format!("{name} = EXCLUDED.{name}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        qb.push(" WHERE ");
        qb.push(
            value_names
                .iter()
                .map(|name| format!("{}.{name} IS DISTINCT FROM EXCLUDED.{name}", R::TABLE))
                .collect::<Vec<_>>()
                .join(" OR "),
        );
    }
    push_returning_key::<R>(&mut qb);
    qb
}

fn retire_stale<'q, R: KnownRow>(
    scope: &[Column],
    kept: &[Vec<String>],
) -> QueryBuilder<'q, Postgres> {
    let mut qb = QueryBuilder::<Postgres>::new("DELETE FROM ");
    qb.push(R::TABLE);
    qb.push(" AS stale WHERE TRUE");
    for column in scope {
        qb.push(" AND stale.");
        qb.push(column.name);
        qb.push(" = ");
        column.value.clone().push_bind_to(&mut qb);
    }
    if kept.first().is_some_and(|column| !column.is_empty()) {
        qb.push(" AND NOT EXISTS (SELECT 1 FROM unnest(");
        for (at, column) in kept.iter().enumerate() {
            if at > 0 {
                qb.push(", ");
            }
            qb.push_bind(column.clone());
            qb.push("::text[]");
        }
        qb.push(") AS kept(");
        qb.push(
            (0..R::KEY.len())
                .map(|at| format!("k{at}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        qb.push(") WHERE ");
        qb.push(
            R::KEY
                .iter()
                .enumerate()
                .map(|(at, name)| format!("kept.k{at} = stale.{name}::text"))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        qb.push(")");
    }
    push_returning_key::<R>(&mut qb);
    qb
}

fn text_key<R: KnownRow>() -> String {
    R::KEY
        .iter()
        .map(|name| format!("{name}::text"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn push_returning_key<R: KnownRow>(qb: &mut QueryBuilder<'_, Postgres>) {
    qb.push(" RETURNING ");
    qb.push(text_key::<R>());
}

fn returned_keys(rows: &[PgRow]) -> Result<Vec<Vec<String>>, EngineError> {
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|at| Ok(row.try_get::<Option<String>, _>(at)?.unwrap_or_default()))
                .collect::<Result<Vec<_>, sqlx::Error>>()
                .map_err(EngineError::from)
        })
        .collect()
}

#[cfg(test)]
mod tests;
