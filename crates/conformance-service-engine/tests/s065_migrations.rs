use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample;
use service_engine::schema::{RESERVED_VERSION_MAX, RESERVED_VERSION_MIN};
use sqlx::PgPool;

#[tokio::test]
async fn s065_the_engine_set_and_a_timestamp_versioned_service_set_apply_in_every_order() {
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
            24,
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

#[tokio::test]
async fn s065_a_legacy_public_outbox_is_drained_into_the_engine_table_then_dropped() {
    let db = TestDb::fresh().await;

    let adopted = db.spare_database("adopt").await;
    create_legacy_outbox(&adopted).await;
    insert_legacy_row(&adopted, "integration.evt.catalog.item.listed.v1").await;
    insert_legacy_row(&adopted, "integration.evt.catalog.item.delisted.v1").await;

    service_engine::schema::migrate(&adopted)
        .await
        .expect("the engine set creates service_engine.integration_outbox");
    sample::migrate(&adopted)
        .await
        .expect("the service set applies on the shared ledger");

    let mut conn = adopted
        .acquire()
        .await
        .expect("a connection for the adoption step");
    service_engine::relays::outbox::adopt_legacy_outbox(&mut conn)
        .await
        .expect("the post-migration step drains the legacy outbox");
    drop(conn);

    assert_eq!(
        engine_outbox_rows(&adopted).await,
        2,
        "both legacy rows were moved into the engine-owned table"
    );
    assert!(
        !table_exists(&adopted, "public.integration_outbox").await,
        "the legacy public table is dropped once its rows are drained"
    );

    let mut again = adopted
        .acquire()
        .await
        .expect("a connection for the second run");
    service_engine::relays::outbox::adopt_legacy_outbox(&mut again)
        .await
        .expect("a second adoption run is a no-op");
    drop(again);
    assert_eq!(
        engine_outbox_rows(&adopted).await,
        2,
        "the second run added nothing"
    );

    let fresh = db.spare_database("fresh").await;
    service_engine::schema::migrate(&fresh)
        .await
        .expect("the engine set applies on an empty database");
    let mut conn = fresh
        .acquire()
        .await
        .expect("a connection on the fresh database");
    service_engine::relays::outbox::adopt_legacy_outbox(&mut conn)
        .await
        .expect("adoption is a no-op when there is no legacy table");
    drop(conn);
    assert!(
        table_exists(&fresh, "service_engine.integration_outbox").await,
        "the fresh database has the one engine-owned outbox table"
    );
    assert!(
        !table_exists(&fresh, "public.integration_outbox").await,
        "no orphan public table is left on a fresh database"
    );

    db.cleanup().await;
}

async fn create_legacy_outbox(pool: &PgPool) {
    sqlx::query(
        "CREATE TABLE public.integration_outbox ( \
           id uuid PRIMARY KEY, subject text NOT NULL, payload jsonb NOT NULL, \
           status text NOT NULL, attempts bigint NOT NULL DEFAULT 0, producer text, \
           seq_key text, seq bigint, last_error text, published_at timestamptz, \
           created_at timestamptz NOT NULL DEFAULT now() )",
    )
    .execute(pool)
    .await
    .expect("create the legacy public outbox the adopters shipped");
}

async fn insert_legacy_row(pool: &PgPool, subject: &str) {
    sqlx::query(
        "INSERT INTO public.integration_outbox (id, subject, payload, status) \
         VALUES ($1, $2, '{}'::jsonb, 'PENDING')",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(subject)
    .execute(pool)
    .await
    .expect("seed a pending legacy row");
}

async fn engine_outbox_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.integration_outbox")
        .fetch_one(pool)
        .await
        .expect("count the engine outbox rows")
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
