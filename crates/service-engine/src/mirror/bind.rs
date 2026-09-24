use sqlx::{Postgres, QueryBuilder};

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

impl Bind {
    pub(super) fn renders_as_its_sql_text(&self) -> bool {
        matches!(
            self,
            Bind::Uuid(_) | Bind::Text(_) | Bind::Int(_) | Bind::Bool(_)
        )
    }

    pub(super) fn fingerprint(&self) -> (u8, String) {
        let tag = match self {
            Bind::Uuid(_) => 0,
            Bind::Text(_) => 1,
            Bind::Int(_) => 2,
            Bind::Real(_) => 3,
            Bind::Bool(_) => 4,
            Bind::Json(_) => 5,
            Bind::Timestamptz(_) => 6,
            Bind::Null => 7,
        };
        (tag, self.render())
    }

    pub(super) fn render(&self) -> String {
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

    pub(super) fn push_bind_to(self, qb: &mut QueryBuilder<'_, Postgres>) {
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
