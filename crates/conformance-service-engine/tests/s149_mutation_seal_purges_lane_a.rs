use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{NoteBody, NoteKey, NoteProjector};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::pipeline::{Mutation, MutationFault, MutationInput};
use service_engine::{Engine, ReadinessHandle};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

const SERVICE: &str = "s149seal";

#[derive(Debug, Deserialize)]
struct SealNote {
    assignment_id: Uuid,
    seq: i32,
}

impl MutationInput for SealNote {
    type Output = ();
    type Error = SealFault;
    const NAME: &'static str = "seal_note";
}

#[derive(Debug)]
struct SealFault(String);

impl std::fmt::Display for SealFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl MutationFault for SealFault {
    fn reason(&self) -> Option<service_engine::gate::Reason> {
        None
    }
}

impl From<EngineError> for SealFault {
    fn from(error: EngineError) -> Self {
        Self(error.to_string())
    }
}

fn seal_note<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: SealNote,
) -> BoxFuture<'m, Result<(), SealFault>> {
    Box::pin(async move {
        let key = NoteKey {
            assignment_id: input.assignment_id,
            seq: input.seq,
        };
        cx.seal_current::<NoteBody>(&key).await?;
        Ok(())
    })
}

async fn purged(pool: &PgPool, key: &NoteKey) -> Option<bool> {
    let key = serde_json::to_value(key).expect("the note key serializes");
    sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
        "SELECT purged_at FROM service_engine.accumulator_seal \
         WHERE accumulator = 'note_body' AND key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await
    .expect("read the seal marker")
    .map(|purged_at| purged_at.is_some())
}

async fn wait_purged(pool: &PgPool, key: &NoteKey) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if purged(pool, key).await == Some(true) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the seal marker was never purged"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

#[tokio::test]
async fn s149_a_sealing_mutation_purges_the_lane_a_key_and_the_beat_backstops_a_stuck_purge() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_streaming(SERVICE, Duration::from_secs(300))
        .await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let mut engine = Engine::boot(
        engine_config("se_s149", "pod-s149")
            .with_service(SERVICE)
            .with_lock_timeout(Duration::from_millis(500)),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the lane-A pipeline engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(NoteProjector)
        .expect("register the note projector, which binds the note noun");
    engine
        .register_accumulator(NoteBody)
        .expect("register the note-body accumulator so lane A binds its STREAMING stream");
    engine
        .register_mutation::<SealNote, _>(seal_note)
        .expect("register the sealing mutation");
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let accumulators = engine.accumulators().clone();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lane-A engine never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let sealed = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };
    for seq in 0..=2u64 {
        accumulators
            .push_chunk::<NoteBody>(
                &sealed,
                service_engine::accumulator::ChunkSeq::new(seq).unwrap(),
                format!("<{seq}>"),
            )
            .expect("the chunk buffers")
            .await
            .expect("the running flush loop makes the chunk durable");
    }

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    executor
        .run::<SealNote>(
            principal,
            SealNote {
                assignment_id: sealed.assignment_id,
                seq: sealed.seq,
            },
        )
        .await
        .expect("the sealing mutation commits through the direct pipeline");

    wait_purged(&pool, &sealed).await;

    let stuck = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 2,
    };
    sqlx::query(
        "INSERT INTO service_engine.accumulator_seal (accumulator, key, high_water, sealed_at, purged_at) \
         VALUES ('note_body', $1, 3, now(), NULL)",
    )
    .bind(serde_json::to_value(&stuck).unwrap())
    .execute(&pool)
    .await
    .expect("seed a seal marker whose synchronous purge is imagined to have failed");
    assert_eq!(
        purged(&pool, &stuck).await,
        Some(false),
        "the seeded marker starts unpurged, standing in for a mutation whose synchronous purge \
         did not land"
    );

    wait_purged(&pool, &stuck).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
