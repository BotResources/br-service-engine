use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    GraphqlService, boot_fact_service, passport_for,
};
use conformance_service_engine::sample::render::member;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use uuid::Uuid;

const FACT_QUERY: &str = "{ sampleRegisteredTenant }";
const REJECTED_BODY: &str = "the passport is rejected: the passport carries no tenant claim";

async fn post(service: &GraphqlService, passport: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(service.http("/graphql"))
        .header("x-passport", passport)
        .json(&serde_json::json!({ "query": FACT_QUERY }))
        .send()
        .await
        .expect("the POST reaches the server");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

async fn refused_upgrade(service: &GraphqlService, passport: &str) -> (u16, String) {
    let mut request = service
        .ws("/graphql/ws")
        .into_client_request()
        .expect("the ws url is a valid client request");
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static("graphql-transport-ws"),
    );
    request.headers_mut().insert(
        "x-passport",
        HeaderValue::from_str(passport).expect("the passport is a valid header value"),
    );
    match tokio_tungstenite::connect_async(request).await {
        Ok(_) => panic!("the upgrade was accepted where the principal could not be resolved"),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => (
            response.status().as_u16(),
            String::from_utf8_lossy(response.body().as_deref().unwrap_or_default()).into_owned(),
        ),
        Err(other) => panic!("the upgrade failed below HTTP: {other}"),
    }
}

fn tenantless_passport(user: Uuid) -> String {
    Passport::human(
        user,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    )
    .to_header()
}

fn assert_coded_internal(channel: &str, status: u16, body: &str, app_role: &str) {
    assert_eq!(
        status, 500,
        "{channel}: a principal fact the pod cannot read is an infrastructure fault, never a 401: {body}"
    );
    let json: serde_json::Value = serde_json::from_str(body)
        .unwrap_or_else(|_| panic!("{channel}: the fault answers the GraphQL error shape: {body}"));
    assert_eq!(
        (
            &json["errors"][0]["extensions"]["code"],
            &json["errors"][0]["message"]
        ),
        (
            &serde_json::json!("INTERNAL"),
            &serde_json::json!("internal error")
        ),
        "{channel}: the fault carries the INTERNAL code and the fixed message: {body}"
    );
    for leak in [
        "permission",
        "denied",
        "sample_member",
        "relation",
        app_role,
    ] {
        assert!(
            !body.contains(leak),
            "{channel}: the client learns the code, never the database text (`{leak}`): {body}"
        );
    }
}

async fn set_fact_grant(db: &TestDb, grant: bool) {
    let statement = if grant {
        format!("GRANT SELECT ON sample_member TO \"{}\"", db.app_role())
    } else {
        format!("REVOKE SELECT ON sample_member FROM \"{}\"", db.app_role())
    };
    sqlx::query(&statement)
        .execute(db.owner_pool())
        .await
        .expect("the owner changes the app role's grant on the fact table");
}

#[tokio::test]
async fn s243_a_fact_loader_fault_answers_a_coded_500_while_a_rejected_passport_keeps_its_401() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let service = boot_fact_service(&db, nats.nats().await, "se_s243", "pod-s243").await;

    let user = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    member(db.owner_pool(), user, tenant).await;
    let passport = passport_for(user, tenant).to_header();
    let rejected = tenantless_passport(user);

    let (status, body) = post(&service, &passport).await;
    assert_eq!(
        status, 200,
        "a healthy fact read serves the request: {body}"
    );
    let json: serde_json::Value = serde_json::from_str(&body).expect("a GraphQL response");
    assert_eq!(
        json["data"]["sampleRegisteredTenant"],
        serde_json::json!(tenant.to_string()),
        "the registered fact loader ran on the request path and its fact reached the resolver: {body}"
    );

    set_fact_grant(&db, false).await;

    let (status, body) = post(&service, &passport).await;
    assert_coded_internal("POST /graphql", status, &body, db.app_role());

    let (status, body) = refused_upgrade(&service, &passport).await;
    assert_coded_internal("GET /graphql/ws", status, &body, db.app_role());

    assert_eq!(
        post(&service, &rejected).await,
        (401, REJECTED_BODY.to_string()),
        "a passport the principal rejects keeps its exact 401 while the fact table faults: \
         rejection is decided before any fact is read"
    );
    assert_eq!(
        refused_upgrade(&service, &rejected).await,
        (401, REJECTED_BODY.to_string()),
        "the WebSocket upgrade answers a rejected passport with the same exact 401"
    );

    set_fact_grant(&db, true).await;

    let (status, body) = post(&service, &passport).await;
    assert_eq!(
        status, 200,
        "the fault is read per request, never cached: the restored grant serves again: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
