use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use crate::error::EngineError;

pub struct Column {
    pub name: &'static str,
    pub value: Bind,
}

pub fn col(name: &'static str, value: impl Into<Bind>) -> Column {
    Column {
        name,
        value: value.into(),
    }
}

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

impl Bind {
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

pub trait KnownRow: Send + Sync + 'static {
    const TABLE: &'static str;
    const NAMESPACE: &'static str;

    fn key(&self) -> Vec<Column>;
    fn values(&self) -> Vec<Column>;

    fn foreign_key(&self) -> String {
        foreign_key_of(&self.key())
    }
}

pub fn foreign_key_of(key: &[Column]) -> String {
    key.iter()
        .map(|c| c.value.render())
        .collect::<Vec<_>>()
        .join("/")
}

pub(super) async fn upsert<R: KnownRow>(
    conn: &mut PgConnection,
    row: &R,
) -> Result<Written, EngineError> {
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
