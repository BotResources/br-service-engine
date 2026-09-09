use std::collections::BTreeSet;

use sqlx::{PgConnection, Row};

use crate::erase::PersonId;
use crate::error::EngineError;
use crate::full_eda::EventSourced;
use crate::full_eda::decode_key_text;
use crate::full_eda::store::{load_aggregate, upsert_snapshot};
use crate::schema::TABLE_EVENT_LOG;

pub async fn erase<T, R>(
    conn: &mut PgConnection,
    person: &PersonId,
    redact: R,
) -> Result<Vec<T::Key>, EngineError>
where
    T: EventSourced,
    R: Fn(&PersonId, &mut T::Event) -> bool + Send,
{
    let rows = sqlx::query(&format!(
        "SELECT key, seq, version, payload FROM {TABLE_EVENT_LOG} WHERE noun = $1 ORDER BY key, seq"
    ))
    .bind(T::NOUN.as_str())
    .fetch_all(&mut *conn)
    .await?;

    let mut touched: BTreeSet<String> = BTreeSet::new();
    for row in &rows {
        let key_text: String = row.get("key");
        let seq: i64 = row.get("seq");
        let version: i32 = row.get("version");
        let payload: serde_json::Value = row.get("payload");
        let mut event = T::upcast(version, &payload)?;
        if !redact(person, &mut event) {
            continue;
        }
        let rewritten = serde_json::to_value(&event).map_err(|source| EngineError::Encode {
            what: "full-eda erased event",
            source,
        })?;
        sqlx::query(&format!(
            "UPDATE {TABLE_EVENT_LOG} SET payload = $4, version = $5 \
             WHERE noun = $1 AND key = $2 AND seq = $3"
        ))
        .bind(T::NOUN.as_str())
        .bind(&key_text)
        .bind(seq)
        .bind(&rewritten)
        .bind(T::EVENT_VERSION)
        .execute(&mut *conn)
        .await?;
        touched.insert(key_text);
    }

    let mut out = Vec::with_capacity(touched.len());
    for key_text in &touched {
        if let Some(aggregate) = load_aggregate::<T>(conn, key_text).await? {
            upsert_snapshot::<T>(conn, key_text, &aggregate).await?;
        }
        out.push(decode_key_text::<T::Key>(key_text)?);
    }
    Ok(out)
}
