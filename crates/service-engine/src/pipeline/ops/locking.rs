use std::any::TypeId;
use std::collections::HashMap;

use sqlx::PgConnection;

use crate::error::EngineError;
use crate::persistence::{Aggregate, Persistence};

fn key_bytes<A: Aggregate>(key: &<A::Store as Persistence>::Key) -> Result<Vec<u8>, EngineError> {
    serde_json::to_vec(key).map_err(|source| EngineError::Encode {
        what: "aggregate key for the load advisory lock",
        source,
    })
}

pub(super) fn render_key<A: Aggregate>(key: &<A::Store as Persistence>::Key) -> String {
    serde_json::to_string(key).unwrap_or_else(|_| "<unrenderable key>".to_string())
}

type KeyWithBytes<A> = (<<A as Aggregate>::Store as Persistence>::Key, Vec<u8>);

pub(super) fn lock_order<A: Aggregate>(
    keys: &[<A::Store as Persistence>::Key],
) -> Result<Vec<KeyWithBytes<A>>, EngineError> {
    let mut ordered: Vec<KeyWithBytes<A>> = Vec::with_capacity(keys.len());
    for key in keys {
        let bytes = key_bytes::<A>(key)?;
        if ordered.iter().any(|(_, seen)| *seen == bytes) {
            continue;
        }
        ordered.push((key.clone(), bytes));
    }
    ordered.sort_by(|(_, a), (_, b)| a.cmp(b));
    Ok(ordered)
}

pub(super) fn in_locked_order<A: Aggregate>(
    loaded: Vec<(<A::Store as Persistence>::Key, A)>,
    order: &[<A::Store as Persistence>::Key],
) -> Vec<A> {
    let mut by_key: HashMap<<A::Store as Persistence>::Key, A> = loaded.into_iter().collect();
    order.iter().filter_map(|key| by_key.remove(key)).collect()
}

pub(super) async fn lock_aggregate<A: Aggregate>(
    conn: &mut PgConnection,
    key: &<A::Store as Persistence>::Key,
) -> Result<(), EngineError> {
    let store = std::any::type_name::<A::Store>();
    let bytes = key_bytes::<A>(key)?;
    let id = crate::advisory::lock_id(crate::advisory::AGGREGATE_LOAD, &[store.as_bytes(), &bytes]);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(EngineError::Db)?;
    A::Store::lock(conn, key).await
}

pub(super) fn reconcile_key<A: Aggregate>(aggregate: &A) -> Result<(TypeId, Vec<u8>), EngineError> {
    let key = serde_json::to_vec(&aggregate.key()).map_err(|source| EngineError::Encode {
        what: "aggregate key for blob reconciliation",
        source,
    })?;
    Ok((TypeId::of::<A>(), key))
}

#[cfg(test)]
mod lock_order_tests {
    use super::*;
    use crate::persistence::PersistenceStyle;
    use futures_util::future::BoxFuture;

    #[derive(Clone)]
    struct Widget(String);
    struct WidgetStore;

    impl Persistence for WidgetStore {
        type Aggregate = Widget;
        type Key = String;
        type Event = ();
        const STYLE: PersistenceStyle = PersistenceStyle::Crud;

        fn read_many<'a>(
            _conn: &'a mut PgConnection,
            _keys: &'a [Self::Key],
        ) -> BoxFuture<'a, Result<Vec<(Self::Key, Self::Aggregate)>, EngineError>> {
            Box::pin(async { unimplemented!("lock_order never loads") })
        }
        fn save<'a>(
            _conn: &'a mut PgConnection,
            _aggregate: &'a Self::Aggregate,
            _events: &'a [Self::Event],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { unimplemented!("lock_order never saves") })
        }
        fn create<'a>(
            _conn: &'a mut PgConnection,
            _aggregate: &'a Self::Aggregate,
            _events: &'a [Self::Event],
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { unimplemented!("lock_order never creates") })
        }
    }

    impl Aggregate for Widget {
        type Store = WidgetStore;
        fn key(&self) -> String {
            self.0.clone()
        }
    }

    fn order(keys: &[&str]) -> Vec<String> {
        lock_order::<Widget>(&keys.iter().map(|k| k.to_string()).collect::<Vec<_>>())
            .expect("string keys always encode")
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    #[test]
    fn keys_are_locked_in_ascending_encoded_order_whatever_the_input_order() {
        assert_eq!(order(&["c", "a", "b"]), vec!["a", "b", "c"]);
        assert_eq!(order(&["b", "c", "a"]), vec!["a", "b", "c"]);
    }

    #[test]
    fn a_repeated_key_is_locked_once() {
        assert_eq!(order(&["a", "b", "a", "b", "a"]), vec!["a", "b"]);
    }

    #[test]
    fn the_empty_set_locks_nothing() {
        assert!(order(&[]).is_empty());
    }

    fn widget(key: &str) -> Widget {
        Widget(key.to_string())
    }

    #[test]
    fn a_batch_returned_out_of_order_is_re_emitted_in_the_locked_order() {
        let loaded = vec![
            ("c".to_string(), widget("c")),
            ("a".to_string(), widget("a")),
            ("b".to_string(), widget("b")),
        ];
        let order = ["a".to_string(), "b".to_string(), "c".to_string()];
        let result: Vec<_> = in_locked_order::<Widget>(loaded, &order)
            .into_iter()
            .map(|w| w.0)
            .collect();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn an_absent_key_is_omitted_and_the_rest_keep_the_locked_order() {
        let loaded = vec![
            ("c".to_string(), widget("c")),
            ("a".to_string(), widget("a")),
        ];
        let order = ["a".to_string(), "b".to_string(), "c".to_string()];
        let result: Vec<_> = in_locked_order::<Widget>(loaded, &order)
            .into_iter()
            .map(|w| w.0)
            .collect();
        assert_eq!(result, vec!["a", "c"]);
    }
}
