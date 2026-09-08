use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample;
use service_engine::schema::{RESERVED_VERSION_MAX, RESERVED_VERSION_MIN};
use sqlx::PgPool;

#[tokio::test]
async fn s24_the_engine_set_and_a_timestamp_versioned_service_set_apply_in_every_order() {
    let db = TestDb::fresh().await;

    let engine_first = db.spare_database("engine_first").await;
    service_engine::schema::migrate(&engine_first)
        .await
        .expect("the engine set applies on an empty ledger");
    sample::migrate(&engine_first)
        .await
        .expect("a timestamp-versioned service set applies after the engine's");

    let service_first = db.spare_database("service_first").await;
    sample::migrate(&service_first)
        .await
        .expect("a timestamp-versioned service set applies on an empty ledger");
    service_engine::schema::migrate(&service_first)
        .await
        .expect("the engine set applies after a service set whose versions it has never seen");

    for pool in [&engine_first, &service_first] {
        assert_eq!(
            reserved_versions(pool).await,
            7,
            "every engine migration is on the shared ledger, inside its reserved range"
        );
        assert!(
            timestamp_versions(pool).await >= 4,
            "the service set is on the same ledger with its own timestamp versions"
        );
        for table in service_engine::schema::TABLES {
            assert!(
                table_exists(pool, table).await,
                "{table} is missing after the two sets applied"
            );
        }
        for table in sample::TABLES {
            assert!(table_exists(pool, table).await, "{table} is missing");
        }
    }

    db.cleanup().await;
}

async fn reserved_versions(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version BETWEEN $1 AND $2")
        .bind(RESERVED_VERSION_MIN)
        .bind(RESERVED_VERSION_MAX)
        .fetch_one(pool)
        .await
        .expect("read the shared migration ledger")
}

async fn timestamp_versions(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version > $1")
        .bind(RESERVED_VERSION_MAX)
        .fetch_one(pool)
        .await
        .expect("read the shared migration ledger")
}

async fn table_exists(pool: &PgPool, table: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(table)
        .fetch_one(pool)
        .await
        .expect("ask PostgreSQL whether the table exists")
}
