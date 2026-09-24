use std::collections::BTreeSet;

use sqlx::{Postgres, QueryBuilder};

use super::bind::{Bind, Column};

pub struct RowScope {
    columns: Vec<ScopeColumn>,
}

enum ScopeColumn {
    Equals(Column),
    AnyOf {
        name: &'static str,
        values: Vec<Bind>,
    },
}

impl RowScope {
    pub fn by(column: Column) -> Self {
        Self::whole_table().and(column)
    }

    pub fn any_of<V: Into<Bind>>(name: &'static str, values: impl IntoIterator<Item = V>) -> Self {
        Self {
            columns: vec![ScopeColumn::AnyOf {
                name,
                values: values.into_iter().map(Into::into).collect(),
            }],
        }
    }

    pub fn and(mut self, column: Column) -> Self {
        self.columns.push(ScopeColumn::Equals(column));
        self
    }

    pub fn whole_table() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub(super) fn into_filters(self) -> Result<Vec<ScopeFilter>, String> {
        self.columns.into_iter().map(ScopeFilter::of).collect()
    }
}

pub(super) struct ScopeFilter {
    name: &'static str,
    accepted: BTreeSet<(u8, String)>,
    bound: Bound,
}

enum Bound {
    One(Bind),
    Many(TypedArray),
    Nothing,
}

#[derive(Clone)]
enum TypedArray {
    Uuid(Vec<uuid::Uuid>),
    Text(Vec<String>),
    Int(Vec<i64>),
    Bool(Vec<bool>),
}

impl ScopeFilter {
    fn of(column: ScopeColumn) -> Result<Self, String> {
        match column {
            ScopeColumn::Equals(Column { name, value }) => {
                if matches!(value, Bind::Null) {
                    return Err(format!(
                        "scope column {name} is null, so it would match no row"
                    ));
                }
                Ok(Self {
                    name,
                    accepted: BTreeSet::from([value.fingerprint()]),
                    bound: Bound::One(value),
                })
            }
            ScopeColumn::AnyOf { name, values } => {
                let accepted = values.iter().map(Bind::fingerprint).collect();
                let bound = if values.is_empty() {
                    Bound::Nothing
                } else {
                    Bound::Many(TypedArray::of(values).ok_or_else(|| {
                        format!(
                            "scope column {name} takes any_of uuid, text, integer or boolean \
                             values of one type"
                        )
                    })?)
                };
                Ok(Self {
                    name,
                    accepted,
                    bound,
                })
            }
        }
    }

    pub(super) fn name(&self) -> &'static str {
        self.name
    }

    pub(super) fn matches_nothing(&self) -> bool {
        matches!(self.bound, Bound::Nothing)
    }

    pub(super) fn carried_by(&self, key: &[Column], values: &[Column]) -> bool {
        key.iter()
            .chain(values)
            .any(|c| c.name == self.name && self.accepted.contains(&c.value.fingerprint()))
    }

    pub(super) fn push_to(&self, qb: &mut QueryBuilder<'_, Postgres>, alias: &str) {
        let column = format!(" AND {alias}.{}", self.name);
        match &self.bound {
            Bound::One(value) => {
                qb.push(column);
                qb.push(" = ");
                value.clone().push_bind_to(qb);
            }
            Bound::Many(array) => {
                qb.push(column);
                qb.push(" = ANY(");
                array.clone().push_bind_to(qb);
                qb.push(")");
            }
            Bound::Nothing => {
                qb.push(" AND FALSE");
            }
        }
    }
}

impl TypedArray {
    fn of(values: Vec<Bind>) -> Option<Self> {
        let mut array = match values.first()? {
            Bind::Uuid(_) => TypedArray::Uuid(Vec::with_capacity(values.len())),
            Bind::Text(_) => TypedArray::Text(Vec::with_capacity(values.len())),
            Bind::Int(_) => TypedArray::Int(Vec::with_capacity(values.len())),
            Bind::Bool(_) => TypedArray::Bool(Vec::with_capacity(values.len())),
            _ => return None,
        };
        for value in values {
            match (&mut array, value) {
                (TypedArray::Uuid(held), Bind::Uuid(v)) => held.push(v),
                (TypedArray::Text(held), Bind::Text(v)) => held.push(v),
                (TypedArray::Int(held), Bind::Int(v)) => held.push(v),
                (TypedArray::Bool(held), Bind::Bool(v)) => held.push(v),
                _ => return None,
            }
        }
        Some(array)
    }

    fn push_bind_to(self, qb: &mut QueryBuilder<'_, Postgres>) {
        match self {
            TypedArray::Uuid(v) => qb.push_bind(v),
            TypedArray::Text(v) => qb.push_bind(v),
            TypedArray::Int(v) => qb.push_bind(v),
            TypedArray::Bool(v) => qb.push_bind(v),
        };
    }
}
