mod graphql_support;

use std::time::{Duration, Instant};

use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_reads_service, passport_for};
use conformance_service_engine::sample::render::{assignment, member};
use graphql_support::GraphqlWs;
use uuid::Uuid;

const WITHIN: Duration = Duration::from_secs(15);
const CONCURRENT: usize = 12;
const JOURNAL: &str = "query { sampleAssignmentJournal }";

async fn collect(ws: &mut GraphqlWs, count: usize) -> Vec<serde_json::Value> {
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        payloads.push(
            ws.next_payload(WITHIN)
                .await
                .expect("every concurrent operation on the socket is answered"),
        );
    }
    payloads
}

fn assert_all_served(label: &str, payloads: &[serde_json::Value]) {
    for payload in payloads {
        assert!(
            payload["errors"].is_null(),
            "{label}: a live principal's read is served, never starved of a connection: {payload}"
        );
        assert!(
            !payload["data"].is_null(),
            "{label}: the answer carries its datum: {payload}"
        );
    }
}

#[tokio::test]
async fn s272_more_concurrent_rls_reads_than_pool_connections_on_one_socket_are_all_served_promptly()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    member(&pool, user, tenant).await;
    let visible = assignment(&pool, tenant, "alpha").await;

    let service = boot_reads_service(&db, nats.nats().await, "pod-s272", true).await;
    let passport = passport_for(user, tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    let started = Instant::now();
    for index in 0..CONCURRENT {
        ws.subscribe(
            &format!("journal-{index}"),
            "query { sampleAssignmentJournal }",
        )
        .await;
    }
    let journals = collect(&mut ws, CONCURRENT).await;
    assert_all_served("sampleAssignmentJournal", &journals);
    for payload in &journals {
        assert_eq!(
            payload["data"]["sampleAssignmentJournal"],
            serde_json::json!([visible]),
            "control: the refreshed principal reads its tenant's rows: {payload}"
        );
    }

    for index in 0..CONCURRENT {
        ws.subscribe(
            &format!("rls-{index}"),
            &format!("query {{ sampleRlsVisibleAssignment(id: \"{visible}\") {{ id }} }}"),
        )
        .await;
    }
    let loads = collect(&mut ws, CONCURRENT).await;
    assert_all_served("sampleRlsVisibleAssignment", &loads);
    for payload in &loads {
        assert_eq!(
            payload["data"]["sampleRlsVisibleAssignment"]["id"],
            serde_json::json!(visible),
            "control: the RLS load answers the visible row: {payload}"
        );
    }
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{} concurrent reads on a pool of 4 finish far below the acquire timeout, took {:?}",
        2 * CONCURRENT,
        started.elapsed()
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

async fn set_member_grant(db: &TestDb, grant: bool) {
    let statement = if grant {
        format!("GRANT SELECT ON sample_member TO \"{}\"", db.app_role())
    } else {
        format!("REVOKE SELECT ON sample_member FROM \"{}\"", db.app_role())
    };
    sqlx::query(&statement)
        .execute(db.owner_pool())
        .await
        .expect("the owner changes the app role grant on the member table");
}

#[tokio::test]
async fn s272_a_principal_store_fault_at_a_socket_operation_is_internal_never_unauthenticated() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let user = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    member(db.owner_pool(), user, tenant).await;
    let visible = assignment(db.app_pool(), tenant, "alpha").await;
    let service = boot_reads_service(&db, nats.nats().await, "pod-s272f", true).await;
    let passport = passport_for(user, tenant).to_header();
    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;

    ws.subscribe("1", JOURNAL).await;
    let healthy = ws
        .next_payload(WITHIN)
        .await
        .expect("a healthy refresh serves the operation");
    assert_eq!(
        healthy["data"]["sampleAssignmentJournal"],
        serde_json::json!([visible]),
        "control: the refresh resolved the principal and the read was served: {healthy}"
    );

    set_member_grant(&db, false).await;

    ws.subscribe("2", JOURNAL).await;
    let faulted = ws
        .next_payload(WITHIN)
        .await
        .expect("the fault is framed as a next payload");
    assert_eq!(
        faulted["errors"][0]["extensions"]["code"],
        serde_json::json!("INTERNAL"),
        "an unreachable principal store is an infrastructure fault, never a revocation: {faulted}"
    );
    assert_eq!(
        faulted["errors"][0]["message"],
        serde_json::json!("internal error"),
        "the client learns the code and the fixed message only: {faulted}"
    );
    let text = faulted.to_string();
    for leak in ["permission", "denied", "sample_member", db.app_role()] {
        assert!(
            !text.contains(leak),
            "the client never learns the database text (`{leak}`): {text}"
        );
    }

    set_member_grant(&db, true).await;

    ws.subscribe("3", JOURNAL).await;
    let restored = ws
        .next_payload(WITHIN)
        .await
        .expect("the same socket is served once the store answers again");
    assert_eq!(
        restored["data"]["sampleAssignmentJournal"],
        serde_json::json!([visible]),
        "the fault is per operation, never cached on the socket: {restored}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
