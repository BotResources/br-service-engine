//! The declarative half of the mirror kit: a `known_*` row that states its
//! table, key columns and value columns, and lets the engine generate the
//! upsert and the delete. A slice that uses it writes no `format!` SQL against a
//! raw connection; the manual [`Known`](super::projection::Known) /
//! [`KnownScope`](super::projection::KnownScope) impls remain the escape hatch
//! for a row whose write is not a plain single-key upsert (a membership set
//! deleted with `RETURNING`, a column type outside [`Bind`]).

use sqlx::{PgConnection, Postgres, QueryBuilder};

use crate::error::EngineError;

/// A column name paired with the value bound to it. The name is a code
/// identifier, never user input, so it is interpolated into the SQL directly;
/// the value is bound as a parameter.
pub struct Column {
    pub name: &'static str,
    pub value: Bind,
}

/// Build a [`Column`] from a name and any value a [`Bind`] accepts.
pub fn col(name: &'static str, value: impl Into<Bind>) -> Column {
    Column {
        name,
        value: value.into(),
    }
}

/// The column value set a `known_*` fact table draws from. A column outside it
/// keeps the manual `Known`/`KnownScope` impl.
#[derive(Clone)]
pub enum Bind {
    Uuid(uuid::Uuid),
    Text(String),
    Int(i64),
    Real(f64),
    Bool(bool),
    Json(serde_json::Value),
    Timestamptz(chrono::DateTime<chrono::Utc>),
    Null,
}

impl Bind {
    /// The string the foreign key is built from, so an upsert and a retire of
    /// the same key produce the same `ForeignKey` a view's `inverse` decodes.
    fn render(&self) -> String {
        match self {
            Bind::Uuid(v) => v.to_string(),
            Bind::Text(v) => v.clone(),
            Bind::Int(v) => v.to_string(),
            Bind::Real(v) => v.to_string(),
            Bind::Bool(v) => v.to_string(),
            Bind::Json(v) => v.to_string(),
            Bind::Timestamptz(v) => v.to_rfc3339(),
            Bind::Null => String::new(),
        }
    }

    /// Bind this value as one parameter onto the query (or the literal `NULL`),
    /// with no surrounding punctuation — the caller places the commas or the
    /// `AND`. The single match from a variant to its `push_bind` lives here,
    /// shared by the upsert's `VALUES` list and the delete's `WHERE` predicate so
    /// the two SQL paths cannot drift.
    fn push_bind_to(self, qb: &mut QueryBuilder<'_, Postgres>) {
        match self {
            Bind::Uuid(v) => {
                qb.push_bind(v);
            }
            Bind::Text(v) => {
                qb.push_bind(v);
            }
            Bind::Int(v) => {
                qb.push_bind(v);
            }
            Bind::Real(v) => {
                qb.push_bind(v);
            }
            Bind::Bool(v) => {
                qb.push_bind(v);
            }
            Bind::Json(v) => {
                qb.push_bind(v);
            }
            Bind::Timestamptz(v) => {
                qb.push_bind(v);
            }
            Bind::Null => {
                qb.push("NULL");
            }
        }
    }
}

impl From<uuid::Uuid> for Bind {
    fn from(v: uuid::Uuid) -> Self {
        Bind::Uuid(v)
    }
}
impl From<String> for Bind {
    fn from(v: String) -> Self {
        Bind::Text(v)
    }
}
impl From<&str> for Bind {
    fn from(v: &str) -> Self {
        Bind::Text(v.to_string())
    }
}
impl From<i64> for Bind {
    fn from(v: i64) -> Self {
        Bind::Int(v)
    }
}
impl From<i32> for Bind {
    fn from(v: i32) -> Self {
        Bind::Int(i64::from(v))
    }
}
impl From<bool> for Bind {
    fn from(v: bool) -> Self {
        Bind::Bool(v)
    }
}
impl From<f64> for Bind {
    fn from(v: f64) -> Self {
        Bind::Real(v)
    }
}
impl From<serde_json::Value> for Bind {
    fn from(v: serde_json::Value) -> Self {
        Bind::Json(v)
    }
}
impl From<chrono::DateTime<chrono::Utc>> for Bind {
    fn from(v: chrono::DateTime<chrono::Utc>) -> Self {
        Bind::Timestamptz(v)
    }
}
impl<T: Into<Bind>> From<Option<T>> for Bind {
    fn from(v: Option<T>) -> Self {
        v.map_or(Bind::Null, Into::into)
    }
}

/// A `known_*` row the engine can write declaratively. `key` and `values`
/// together are every column of one row; `key` is the conflict target and the
/// delete predicate, `values` the columns refreshed on conflict.
pub trait KnownRow: Send + Sync + 'static {
    const TABLE: &'static str;
    const NAMESPACE: &'static str;

    fn key(&self) -> Vec<Column>;
    fn values(&self) -> Vec<Column>;

    /// The foreign key the impact carries; the key columns joined by default,
    /// which matches a retire of the same key.
    fn foreign_key(&self) -> String {
        foreign_key_of(&self.key())
    }
}

/// The foreign key a set of key columns renders to — shared by an upsert and a
/// retire so both stage the same `ForeignKey`.
pub fn foreign_key_of(key: &[Column]) -> String {
    key.iter()
        .map(|c| c.value.render())
        .collect::<Vec<_>>()
        .join("/")
}

/// `INSERT … ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, or
/// `DO NOTHING` when the row is all key.
pub(super) async fn upsert<R: KnownRow>(
    conn: &mut PgConnection,
    row: &R,
) -> Result<(), EngineError> {
    let key = row.key();
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
        let mut sets = qb.separated(", ");
        for name in &value_names {
            sets.push(format!("{name} = EXCLUDED.{name}"));
        }
    }
    qb.build().execute(conn).await?;
    Ok(())
}

/// `DELETE FROM table WHERE key = $…`.
pub(super) async fn delete_by_key<R: KnownRow>(
    conn: &mut PgConnection,
    key: Vec<Column>,
) -> Result<(), EngineError> {
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
    qb.build().execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct KnownPersonRow {
        id: Uuid,
        email: String,
        note: Option<String>,
    }

    impl KnownRow for KnownPersonRow {
        const TABLE: &'static str = "known_persons";
        const NAMESPACE: &'static str = "example.person";

        fn key(&self) -> Vec<Column> {
            vec![col("user_id", self.id)]
        }

        fn values(&self) -> Vec<Column> {
            vec![
                col("email", self.email.clone()),
                col("note", self.note.clone()),
            ]
        }
    }

    #[test]
    fn the_foreign_key_of_a_single_key_row_is_the_key_value() {
        let id = Uuid::now_v7();
        let row = KnownPersonRow {
            id,
            email: "a@example.test".to_string(),
            note: None,
        };
        assert_eq!(KnownRow::foreign_key(&row), id.to_string());
    }

    #[test]
    fn a_multi_key_foreign_key_joins_its_columns() {
        let key = vec![col("group_id", "g"), col("user_id", "u")];
        assert_eq!(foreign_key_of(&key), "g/u");
    }

    #[test]
    fn a_none_option_binds_null() {
        assert!(matches!(Bind::from(Option::<String>::None), Bind::Null));
        assert!(matches!(Bind::from(Some(4_i64)), Bind::Int(4)));
    }
}
