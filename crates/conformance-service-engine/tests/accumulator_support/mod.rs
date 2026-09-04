use std::sync::Arc;

use conformance_service_engine::sample::{NoteBody, NoteBodyState, NoteKey};
use service_engine::accumulator::{Accumulated, AccumulatorRuntime};
use sqlx::{PgPool, Row};

pub async fn state(engine: &Arc<AccumulatorRuntime>, key: &NoteKey) -> Accumulated<NoteBodyState> {
    engine
        .reader()
        .state::<NoteBody>(key)
        .await
        .expect("the folded state is readable")
}

pub async fn rows(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) AS n FROM service_engine.accumulator_chunk")
        .fetch_one(pool)
        .await
        .expect("the chunk table is readable")
        .get("n")
}
