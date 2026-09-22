use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::CrudCounter;
use service_engine::persistence::{Aggregate, Persistence};
use sqlx::PgPool;
use uuid::Uuid;

async fn seed(pool: &PgPool, key: Uuid) {
    sqlx::query(
        "INSERT INTO sample_counter_crud (id, tenant, total, closed) VALUES ($1, $2, 0, false)",
    )
    .bind(key)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await
    .expect("seed the crud counter row");
}

async fn nowait_lock_error(pool: &PgPool, key: Uuid) -> Option<String> {
    let mut conn = pool.acquire().await.expect("a second connection");
    match sqlx::query("SELECT 1 FROM sample_counter_crud WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(key)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => None,
        Err(error) => Some(
            error
                .as_database_error()
                .and_then(|db| db.code().map(|code| code.into_owned()))
                .unwrap_or_default(),
        ),
    }
}

#[tokio::test]
async fn s226_the_row_lock_helper_holds_an_exclusive_row_lock_released_on_commit() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let key = Uuid::now_v7();
    seed(&pool, key).await;

    let mut holder = pool.begin().await.expect("open the holder transaction");
    <CrudCounter as Aggregate>::Store::row_lock(&mut holder, "sample_counter_crud", &key)
        .await
        .expect("the row_lock helper takes the FOR UPDATE lock");

    assert_eq!(
        nowait_lock_error(&pool, key).await.as_deref(),
        Some("55P03"),
        "a second connection cannot lock the same row while the helper holds it; \
         Postgres reports lock_not_available (55P03) rather than blocking under NOWAIT",
    );

    holder.commit().await.expect("commit releases the row lock");

    assert_eq!(
        nowait_lock_error(&pool, key).await,
        None,
        "once the holding transaction commits, the row is lockable again",
    );

    db.cleanup().await;
}
