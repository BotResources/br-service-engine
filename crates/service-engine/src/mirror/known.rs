use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use crate::error::EngineError;

use super::bind::Column;

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

pub trait KnownRow: Send + Sync + 'static {
    const TABLE: &'static str;
    const NAMESPACE: &'static str;
    const KEY: &'static [&'static str];

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
    Ok(key)
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
) -> Result<(), EngineError> {
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
    qb.build().execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mirror::bind::{Bind, col};
    use uuid::Uuid;

    struct KnownPersonRow {
        id: Uuid,
        email: String,
        note: Option<String>,
    }

    impl KnownRow for KnownPersonRow {
        const TABLE: &'static str = "known_persons";
        const NAMESPACE: &'static str = "example.person";
        const KEY: &'static [&'static str] = &["user_id"];

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
        assert_eq!(foreign_key_of(&row.key()), id.to_string());
    }

    #[test]
    fn a_key_that_names_other_columns_than_the_declared_key_is_refused() {
        let refused = keyed::<KnownPersonRow>(vec![col("email", "a@example.test")]);
        assert!(matches!(refused, Err(EngineError::Config(_))));
        let accepted = keyed::<KnownPersonRow>(vec![col("user_id", Uuid::now_v7())]);
        assert!(accepted.is_ok());
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
