use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::offer::WIDGET_OFFER;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    MintSecret, PublishedWidget, WIDGET_OFFER_PREFIX, boot_offer_engine_reconciling, offer_bucket,
    offer_dirty_keys, widget_key,
};
use service_engine::Nats;
use service_engine::impact::Impact;
use service_engine::inbound::DEAD_LETTER_NOUN;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const CONFIDENTIAL: &str = "confidential-token-9921";

#[tokio::test]
async fn s281_an_undecodable_offer_value_is_dead_lettered_and_the_offer_keeps_publishing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s281",
        "pod-s281",
        Duration::from_millis(150),
    )
    .await;
    let shutdown = engine.shutdown_handle();
    let ops_impacts = collect_ops_impacts(&db, "se_s281").await;
    let executor = engine.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let alpha = mint(&executor, &principal, tenant, "alpha").await;
    let beta = mint(&executor, &principal, tenant, "beta").await;
    await_label(&fabric, alpha, Some("alpha")).await;
    await_label(&fabric, beta, Some("beta")).await;

    let orphan_syntax = raw_key(Uuid::now_v7());
    let orphan_shape = raw_key(Uuid::now_v7());
    let beta_key = widget_key(beta).expect("a valid offer key");
    nats.put_raw(
        beta_key.as_str(),
        format!("not json {CONFIDENTIAL}").as_bytes(),
    )
    .await;
    nats.put_raw(&orphan_syntax, format!("{{{{ {CONFIDENTIAL}").as_bytes())
        .await;
    nats.put_raw(
        &orphan_shape,
        format!("{{\"id\": [\"{CONFIDENTIAL}\"]}}").as_bytes(),
    )
    .await;

    await_label(&fabric, beta, Some("beta")).await;
    await_absent(&nats, &orphan_syntax).await;
    await_absent(&nats, &orphan_shape).await;

    let gamma = mint(&executor, &principal, tenant, "gamma").await;
    await_label(&fabric, gamma, Some("gamma")).await;
    await_label(&fabric, alpha, Some("alpha")).await;
    await_drained(&pool, gamma).await;

    let mut subjects = dead_letters(&pool).await;
    subjects.sort();
    let mut expected = vec![
        beta_key.as_str().to_string(),
        orphan_syntax.clone(),
        orphan_shape.clone(),
    ];
    expected.sort();
    assert_eq!(
        subjects.iter().map(|(s, _)| s.clone()).collect::<Vec<_>>(),
        expected,
        "exactly one dead letter per bad key, across every reconcile that saw it"
    );
    assert!(
        subjects
            .iter()
            .all(|(_, error)| error.starts_with("undecodable_value")
                && !error.contains(CONFIDENTIAL)),
        "every reason carries the stable code and never the value, got {subjects:?}"
    );

    let before = last_seen(&pool, &orphan_syntax).await;
    nats.put_raw(&orphan_syntax, b"bad again").await;
    await_seen_again(&pool, &orphan_syntax, before).await;
    await_absent(&nats, &orphan_syntax).await;
    assert_eq!(
        dead_letters(&pool).await.len(),
        3,
        "a bad key written again updates its row; it never adds one"
    );

    await_ops_impacts(&ops_impacts, 3).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

fn raw_key(id: Uuid) -> String {
    format!("{WIDGET_OFFER_PREFIX}{id}")
}

async fn mint(
    executor: &service_engine::pipeline::MutationExecutor<
        conformance_service_engine::sample::SamplePrincipal,
    >,
    principal: &conformance_service_engine::sample::SamplePrincipal,
    tenant: Uuid,
    label: &str,
) -> Uuid {
    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal.clone(),
            MintSecret {
                id,
                label: label.to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits, staging the offer dirty key in the same transaction");
    id
}

async fn await_label(nats: &Nats, id: Uuid, want: Option<&str>) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let observed = tolerant_get(nats, id).await;
        if observed.as_ref().map(|w| w.label.as_str()) == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "widget {id} never reached {want:?}"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_absent(nats: &TestNats, key: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if !nats.holds(key).await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the offer never retracted the undecodable value under {key}"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn dead_letters(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT subject, error FROM service_engine.dead_letter \
         WHERE source = 'mirror' AND reaction = $1 ORDER BY subject",
    )
    .bind(WIDGET_OFFER)
    .fetch_all(pool)
    .await
    .expect("read the offer dead letters")
}

async fn last_seen(pool: &PgPool, key: &str) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar(
        "SELECT last_seen FROM service_engine.dead_letter \
         WHERE source = 'mirror' AND reaction = $1 AND subject = $2",
    )
    .bind(WIDGET_OFFER)
    .bind(key)
    .fetch_one(pool)
    .await
    .expect("read when the dead letter was last seen")
}

async fn await_seen_again(pool: &PgPool, key: &str, before: chrono::DateTime<chrono::Utc>) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if last_seen(pool, key).await > before {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the offer never saw the rewritten bad value"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn tolerant_get(nats: &Nats, id: Uuid) -> Option<PublishedWidget> {
    let key = widget_key(id).expect("a valid offer key");
    offer_bucket(nats).await.get(&key).await.ok().flatten()
}

async fn await_drained(pool: &PgPool, id: Uuid) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if offer_dirty_keys(pool, id).await == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the offer stopped draining its markers while bad values sit under its prefix"
        );
        tokio::time::sleep(POLL).await;
    }
}

type OpsImpacts = std::sync::Arc<std::sync::Mutex<std::collections::BTreeSet<String>>>;

async fn collect_ops_impacts(db: &TestDb, channel: &str) -> OpsImpacts {
    let mut listener = PgListener::connect(&db.url_as(db.app_role()))
        .await
        .expect("a listener on the fresh database");
    listener
        .listen(channel)
        .await
        .expect("LISTEN on the engine channel");
    let seen = OpsImpacts::default();
    let sink = seen.clone();
    tokio::spawn(async move {
        while let Ok(frame) = listener.recv().await {
            let Ok(serde_json::Value::Object(mut envelope)) =
                serde_json::from_str::<serde_json::Value>(frame.payload())
            else {
                continue;
            };
            let Some(Ok(impacts)) = envelope
                .remove("i")
                .map(serde_json::from_value::<Vec<Impact>>)
            else {
                continue;
            };
            for impact in impacts {
                if let Impact::ResourceChanged { noun, key, .. } = impact
                    && noun == DEAD_LETTER_NOUN
                {
                    sink.lock().expect("impact sink").insert(format!("{key:?}"));
                }
            }
        }
    });
    seen
}

async fn await_ops_impacts(seen: &OpsImpacts, wanted: usize) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if seen.lock().expect("impact sink").len() >= wanted {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "each dead letter stages an ops-view impact, got {:?}",
            seen.lock().expect("impact sink")
        );
        tokio::time::sleep(POLL).await;
    }
}
