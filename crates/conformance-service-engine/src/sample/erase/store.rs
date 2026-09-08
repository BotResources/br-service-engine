use service_engine::error::EngineError;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

pub async fn seed_memo(pool: &PgPool, id: Uuid, owner: Uuid, tenant: Uuid, body: &str) {
    sqlx::query("INSERT INTO sample_erase_memo (id, owner, tenant, body) VALUES ($1, $2, $3, $4)")
        .bind(id)
        .bind(owner)
        .bind(tenant)
        .bind(body)
        .execute(pool)
        .await
        .expect("seed the soft-EDA memo");
    sqlx::query("INSERT INTO sample_erase_memo_fact (memo, owner, fact) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(owner)
        .bind(serde_json::json!({ "wrote": body, "owner": owner }))
        .execute(pool)
        .await
        .expect("append the memo fact that carries the person's value");
}

pub async fn count_memos(pool: &PgPool, owner: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_erase_memo WHERE owner = $1")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("count the person's memos")
}

pub async fn count_memo_facts(pool: &PgPool, owner: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_erase_memo_fact WHERE owner = $1")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("count the person's memo facts")
}

pub(crate) async fn erase_memos(conn: &mut PgConnection, owner: Uuid) -> Result<u64, EngineError> {
    sqlx::query("DELETE FROM sample_erase_memo_fact WHERE owner = $1")
        .bind(owner)
        .execute(&mut *conn)
        .await?;
    let deleted = sqlx::query("DELETE FROM sample_erase_memo WHERE owner = $1")
        .bind(owner)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(deleted)
}

pub async fn seed_ledger(pool: &PgPool, subject: Uuid, tenant: Uuid, author: Uuid, total: i64) {
    sqlx::query(
        "INSERT INTO sample_erase_ledger_event (subject, seq, author, payload) \
         VALUES ($1, 1, $2, $3)",
    )
    .bind(subject)
    .bind(author)
    .bind(serde_json::json!({ "author": author, "amount": total }))
    .execute(pool)
    .await
    .expect("append the full-EDA ledger event authored by the person");
    sqlx::query(
        "INSERT INTO sample_erase_ledger_snapshot (subject, tenant, author, total, version) \
         VALUES ($1, $2, $3, $4, 1)",
    )
    .bind(subject)
    .bind(tenant)
    .bind(author)
    .bind(total)
    .execute(pool)
    .await
    .expect("write the full-EDA ledger snapshot");
}

pub async fn count_ledger_by_author(pool: &PgPool, author: Uuid) -> i64 {
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_erase_ledger_event WHERE author = $1")
            .bind(author)
            .fetch_one(pool)
            .await
            .expect("count the person's ledger events");
    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_erase_ledger_snapshot WHERE author = $1")
            .bind(author)
            .fetch_one(pool)
            .await
            .expect("count the person's ledger snapshots");
    events + snapshots
}

pub(crate) async fn erase_ledger(
    conn: &mut PgConnection,
    author: Uuid,
) -> Result<u64, EngineError> {
    let subjects: Vec<Uuid> =
        sqlx::query("SELECT DISTINCT subject FROM sample_erase_ledger_event WHERE author = $1")
            .bind(author)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(|row| row.get::<Uuid, _>("subject"))
            .collect();
    sqlx::query(
        "UPDATE sample_erase_ledger_event \
         SET author = $2, payload = jsonb_set(payload, '{author}', to_jsonb($2::uuid)) \
         WHERE author = $1",
    )
    .bind(author)
    .bind(Uuid::nil())
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "UPDATE sample_erase_ledger_snapshot \
         SET author = $2, version = version + 1 WHERE author = $1",
    )
    .bind(author)
    .bind(Uuid::nil())
    .execute(&mut *conn)
    .await?;
    Ok(subjects.len() as u64)
}
