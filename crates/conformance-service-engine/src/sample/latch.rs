use std::time::Duration;

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

pub struct AdvisoryLatch {
    conn: PoolConnection<Postgres>,
    key: i32,
}

impl AdvisoryLatch {
    pub async fn hold(pool: &PgPool) -> Self {
        let key = i32::try_from(Uuid::now_v7().as_u128() & 0x7fff_ffff)
            .expect("a 31-bit key fits an i32");
        let mut conn = pool
            .acquire()
            .await
            .expect("a connection to hold the latch");
        sqlx::query("SELECT pg_advisory_lock($1::bigint)")
            .bind(key)
            .execute(&mut *conn)
            .await
            .expect("take the latch");
        Self { conn, key }
    }

    pub fn key(&self) -> i32 {
        self.key
    }

    pub async fn await_waiter(&mut self, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
                 AND NOT granted AND database = (SELECT oid FROM pg_database \
                 WHERE datname = current_database()) AND classid = 0 \
                 AND objid = $1::bigint::oid AND objsubid = 1)",
            )
            .bind(self.key)
            .fetch_one(&mut *self.conn)
            .await
            .expect("read the waiters on the latch");
            if waiting {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no transaction came to wait on the latch within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn release(mut self) {
        sqlx::query("SELECT pg_advisory_unlock($1::bigint)")
            .bind(self.key)
            .execute(&mut *self.conn)
            .await
            .expect("release the latch");
    }
}
