use futures_util::future::BoxFuture;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use sqlx::PgPool;
use uuid::Uuid;

use crate::sample::erase::slice::EraseFault;

pub struct SecretEraser;

impl Erasable for SecretEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move {
            let deleted = sqlx::query("DELETE FROM sample_erase_secret WHERE owner = $1")
                .bind(person.as_uuid())
                .execute(cx.connection())
                .await?
                .rows_affected();
            let mut out = Erased::new();
            out.rows(deleted);
            Ok(out)
        })
    }
}

pub async fn seed_secret(pool: &PgPool, id: Uuid, owner: Uuid, tenant: Uuid, body: &str) {
    let mut tx = pool.begin().await.expect("open a seeding transaction");
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await
        .expect("set the tenant context so the strict policy admits the insert");
    sqlx::query(
        "INSERT INTO sample_erase_secret (id, owner, tenant, body) VALUES ($1, $2, $3, $4)",
    )
    .bind(id)
    .bind(owner)
    .bind(tenant)
    .bind(body)
    .execute(&mut *tx)
    .await
    .expect("seed the strict-RLS secret row under its tenant context");
    tx.commit().await.expect("commit the seeded secret");
}

pub async fn count_secrets(pool: &PgPool, tenant: Uuid, owner: Uuid) -> i64 {
    let mut tx = pool.begin().await.expect("open a counting transaction");
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await
        .expect("set the tenant context so the strict policy makes the rows visible");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_erase_secret WHERE owner = $1")
            .bind(owner)
            .fetch_one(&mut *tx)
            .await
            .expect("count the person's secret rows under the tenant context");
    tx.commit().await.expect("close the counting transaction");
    count
}
