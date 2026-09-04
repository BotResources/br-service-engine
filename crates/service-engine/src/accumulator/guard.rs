use sqlx::{PgConnection, Row};

use crate::advisory;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::wire::KeyBytes;

pub(crate) type StreamKey = (AccumulatorName, KeyBytes);

pub(crate) fn lock_id(stream: &StreamKey) -> i64 {
    let (accumulator, key) = stream;
    advisory::lock_id(
        advisory::ACCUMULATOR_STREAM,
        &[accumulator.as_str().as_bytes(), key.as_slice()],
    )
}

pub(crate) async fn hold(
    conn: &mut PgConnection,
    streams: &[StreamKey],
) -> Result<(), EngineError> {
    for stream in streams {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_id(stream))
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

pub(crate) async fn read_seals(
    conn: &mut PgConnection,
    streams: &[StreamKey],
) -> Result<Vec<Option<u64>>, EngineError> {
    let mut names: Vec<String> = Vec::with_capacity(streams.len());
    let mut keys: Vec<serde_json::Value> = Vec::with_capacity(streams.len());
    for (accumulator, key) in streams {
        names.push(accumulator.as_str().to_string());
        keys.push(key.decode::<serde_json::Value>()?);
    }
    let rows = sqlx::query(
        "SELECT u.ord, s.high_water \
         FROM unnest($1::text[], $2::jsonb[]) WITH ORDINALITY AS u(accumulator, key, ord) \
         JOIN service_engine.accumulator_seal s \
           ON s.accumulator = u.accumulator AND s.key = u.key",
    )
    .bind(&names)
    .bind(&keys)
    .fetch_all(&mut *conn)
    .await?;

    let mut sealed = vec![None; streams.len()];
    for row in rows {
        let ordinal = row.get::<i64, _>("ord");
        let slot = usize::try_from(ordinal - 1)
            .ok()
            .and_then(|index| sealed.get_mut(index))
            .ok_or_else(|| {
                EngineError::Db(sqlx::Error::Protocol(format!(
                    "accumulator_seal lookup returned ordinal {ordinal} outside the {} streams it was asked about",
                    streams.len()
                )))
            })?;
        *slot = Some(row.get::<i64, _>("high_water").max(0) as u64);
    }
    Ok(sealed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name::NounName;
    use crate::wire::{Noun, encode_key};

    struct Token;

    impl Noun for Token {
        type Key = String;
        const NAME: NounName = NounName::from_static("token");
    }

    fn stream(accumulator: &'static str, key: &str) -> StreamKey {
        (
            AccumulatorName::from_static(accumulator),
            encode_key::<Token>(&key.to_string()).expect("a string key encodes"),
        )
    }

    #[test]
    fn one_stream_always_hashes_to_the_same_lock_so_two_pods_serialise_on_it() {
        assert_eq!(
            lock_id(&stream("tokens", "a")),
            lock_id(&stream("tokens", "a"))
        );
        assert_eq!(lock_id(&stream("tokens", "a")), -1_557_587_697_064_234_372);
    }

    #[test]
    fn the_accumulator_and_the_key_are_separated_so_a_split_cannot_collide() {
        assert_ne!(
            lock_id(&stream("tokens", "a")),
            lock_id(&stream("token", "sa"))
        );
        assert_ne!(
            lock_id(&stream("tokens", "a")),
            lock_id(&stream("tokens", "b"))
        );
    }
}
