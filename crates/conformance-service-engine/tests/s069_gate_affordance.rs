#[allow(dead_code)]
mod engine_twin;

use std::collections::BTreeMap;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::GatedAssignmentProjector;
use conformance_service_engine::sample::assignment::AssignmentRow;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::*;
use engine_twin::{SOON, await_ready, spy_engine, stage};
use serde::Deserialize;
use service_engine::gate::{ActionName, Gated, check_gates_match_affordances};
use service_engine::impact::Dims;
use uuid::Uuid;

const CLOSE: ActionName = ActionName::from_static("close");
const REOPEN: ActionName = ActionName::from_static("reopen");

#[derive(Debug, Deserialize)]
struct GateDto {
    allowed: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GatedViewDto {
    closed: bool,
    affordances: BTreeMap<String, GateDto>,
}

fn member_principal(tenant: Uuid) -> SamplePrincipal {
    SamplePrincipal::new(Uuid::now_v7(), tenant)
}

fn open_row(tenant: Uuid) -> AssignmentRow {
    AssignmentRow {
        id: Uuid::now_v7(),
        tenant_id: tenant,
        title: "alpha".to_string(),
        closed: false,
    }
}

#[test]
fn s069_the_declared_gate_set_equals_the_rendered_affordance_surface() {
    let tenant = Uuid::now_v7();
    let principal = member_principal(tenant);
    let open = open_row(tenant);
    let mut closed = open.clone();
    closed.closed = true;

    assert_eq!(AssignmentRow::ACTIONS, &[CLOSE, REOPEN]);
    let surface: Vec<ActionName> = open.affordances(&principal).names().collect();
    assert_eq!(surface, vec![CLOSE, REOPEN]);

    check_gates_match_affordances(&open, &principal)
        .expect("the open state agrees between gate and affordance");
    check_gates_match_affordances(&closed, &principal)
        .expect("the closed state agrees between gate and affordance");
}

#[test]
fn s069_a_blocked_affordance_and_the_rejected_mutation_carry_the_same_reason_code() {
    let tenant = Uuid::now_v7();
    let principal = member_principal(tenant);
    let mut row = open_row(tenant);
    row.closed = true;

    let shown = row
        .affordances(&principal)
        .get(CLOSE)
        .expect("close is part of the surface");
    let refused = row
        .close(&principal)
        .expect_err("closing an already closed assignment is refused");

    assert_eq!(
        shown.reason().expect("the shown gate is blocked").code(),
        "ALREADY_CLOSED"
    );
    assert_eq!(refused.code(), "ALREADY_CLOSED");
    assert_eq!(shown.reason().map(|r| r.code()), Some(refused.code()));
}

#[tokio::test]
async fn s069_engine_an_affordance_flip_on_a_state_change_reaches_the_session_as_an_upsert() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config("se_s069_flip", "pod-s069"),
        GatedAssignmentProjector::keys(),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(GatedAssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the gated session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let opening = reset_views(&reset);
    assert_eq!(assignment_ids(opening), vec![subject]);
    let opened: GatedViewDto = opening[0].view.decode().expect("the opening view decodes");
    assert!(!opened.closed);
    assert!(
        opened.affordances["close"].allowed,
        "an open assignment can be closed"
    );
    assert!(!opened.affordances["reopen"].allowed);
    assert_eq!(
        opened.affordances["reopen"].reason.as_deref(),
        Some("NOT_CLOSED")
    );

    sqlx::query("UPDATE sample_assignment SET closed = true WHERE id = $1")
        .bind(subject)
        .execute(&pool)
        .await
        .expect("close the assignment in the store");
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&subject, Dims::EMPTY)],
    )
    .await;

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("closing the assignment flips its affordances and reaches the session");
    assert_eq!(delta.revision().get(), 2);
    let flipped: GatedViewDto = upserted(&delta)
        .view
        .decode()
        .expect("the flipped view decodes");
    assert!(flipped.closed);
    assert!(
        !flipped.affordances["close"].allowed,
        "a closed assignment can no longer be closed"
    );
    assert_eq!(
        flipped.affordances["close"].reason.as_deref(),
        Some("ALREADY_CLOSED"),
        "the flipped affordance carries the same code the mutation would refuse with"
    );
    assert!(
        flipped.affordances["reopen"].allowed,
        "a closed assignment can now be reopened"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
