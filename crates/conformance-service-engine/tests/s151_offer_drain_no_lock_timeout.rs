use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{CloseWidget, boot_offer_engine};
use sqlx::PgPool;
use uuid::Uuid;

const BACKLOG: usize = 800;
const CONTENDING_MUTATIONS: usize = 100;

#[tokio::test]
async fn s151_a_mutation_on_an_offered_noun_commits_while_a_large_drain_is_in_flight() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let ids = seed_backlog(&pool, tenant, BACKLOG).await;

    let leader = boot_offer_engine(&db, nats.nats().await, "se_s146", "pod-s146a").await;
    let standby = boot_offer_engine(&db, nats.nats().await, "se_s146", "pod-s146b").await;
    let leader_shutdown = leader.shutdown_handle();
    let standby_shutdown = standby.shutdown_handle();
    let executor = leader.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let leader_run = tokio::spawn(leader.run());
    let standby_run = tokio::spawn(standby.run());

    let contending: Vec<Uuid> = ids.into_iter().take(CONTENDING_MUTATIONS).collect();
    let outcomes = futures_util::future::join_all(
        contending
            .into_iter()
            .map(|id| executor.run::<CloseWidget>(principal.clone(), CloseWidget { id })),
    )
    .await;

    for outcome in outcomes {
        outcome.expect(
            "a mutation on an offered noun commits while both engines drain a large backlog: the \
             leader claims and resolves in a short transaction and does its KV round-trips outside \
             it, so the mutation's dirty-key upsert never waits on the drain and never reaches \
             lock_timeout",
        );
    }

    leader_shutdown.notify_one();
    standby_shutdown.notify_one();
    leader_run.await.expect("join").expect("run returns Ok");
    standby_run.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn seed_backlog(pool: &PgPool, tenant: Uuid, count: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(count);
    let mut tx = pool.begin().await.expect("open the seeding transaction");
    for n in 0..count {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO sample_widget (id, tenant_id, label, closed) VALUES ($1, $2, $3, false)",
        )
        .bind(id)
        .bind(tenant)
        .bind(format!("backlog-{n}"))
        .execute(&mut *tx)
        .await
        .expect("seed a widget the boot reconcile will stage as a dirty offer key");
        ids.push(id);
    }
    tx.commit().await.expect("commit the seeded backlog");
    ids
}
