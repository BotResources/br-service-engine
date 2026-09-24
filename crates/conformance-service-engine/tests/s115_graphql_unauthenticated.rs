mod graphql_support;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    SAMPLE_REFUSED, boot_forbidden_service, boot_graphql_service, passport_for,
};
use graphql_support::{GraphqlWs, post_json};
use uuid::Uuid;

#[tokio::test]
async fn s115_a_request_without_a_trusted_passport_is_rejected_before_the_pipeline() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s115", "pod-s115").await;

    let widget = Uuid::now_v7();
    let mutation = format!("mutation {{ sampleCloseWidget(id: \"{widget}\") {{ success }} }}");
    let (status, _body) =
        post_json(&service.base_url, None, &mutation, serde_json::json!({})).await;
    assert_eq!(
        status, 401,
        "the kit does authZ only: a request with no X-Passport never reaches a resolver"
    );

    let (status, _body) = post_json(
        &service.base_url,
        Some("not-a-valid-passport"),
        &mutation,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 401,
        "a malformed X-Passport fails closed rather than defaulting to a principal"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s115_a_resolver_refusal_is_a_coded_graphql_error_never_a_transport_error() {
    use br_core_auth::PassportHeader;

    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let service = boot_forbidden_service(&db, nats.nats().await, "se_s115f", "pod-s115f").await;
    let passport = passport_for(Uuid::now_v7(), Uuid::now_v7()).to_header();

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        "query { sampleForbiddenPeek }",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 200,
        "an authenticated resolver refusal answers 200 with a graphql error, never a transport code: {body}"
    );
    assert!(
        body["data"].is_null() || body["data"]["sampleForbiddenPeek"].is_null(),
        "a refused query carries no datum: {body}"
    );
    assert_eq!(
        body["errors"][0]["extensions"]["code"],
        serde_json::json!("FORBIDDEN"),
        "the refusal carries the FORBIDDEN code in extensions, the same shape a mutation refusal uses: {body}"
    );

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    ws.subscribe("s115f", "subscription { sampleForbiddenStream }")
        .await;
    let payload = ws.next_payload(Duration::from_secs(5)).await.expect(
        "a refused subscription open frames the error as a next payload, not a transport error",
    );
    assert!(
        payload["data"].is_null(),
        "a refused subscription open carries no datum: {payload}"
    );
    assert_eq!(
        payload["errors"][0]["extensions"]["code"],
        serde_json::json!("FORBIDDEN"),
        "the subscription-open refusal carries the same FORBIDDEN code: {payload}"
    );
    ws.expect_complete(Duration::from_secs(5)).await;

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s115_a_refusal_forwarded_with_question_mark_answers_its_reason_code() {
    use br_core_auth::PassportHeader;

    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let service = boot_forbidden_service(&db, nats.nats().await, "se_s115q", "pod-s115q").await;
    let passport = passport_for(Uuid::now_v7(), Uuid::now_v7()).to_header();
    let refused = serde_json::json!(SAMPLE_REFUSED.code());

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        "query { sampleRefusedPeek }",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "a refusal is a graphql error: {body}");
    assert!(
        body["data"].is_null() || body["data"]["sampleRefusedPeek"].is_null(),
        "a refused query carries no datum: {body}"
    );
    assert_eq!(
        body["errors"][0]["extensions"]["code"], refused,
        "a blocked gate forwarded with a bare `?` carries its reason in the code extension: {body}"
    );

    let mut ws = GraphqlWs::connect(&service.base_url, &passport).await;
    ws.subscribe("s115q", "subscription { sampleRefusedStream }")
        .await;
    let payload = ws
        .next_payload(Duration::from_secs(5))
        .await
        .expect("a refused subscription open frames the error as a next payload");
    assert_eq!(
        payload["errors"][0]["extensions"]["code"], refused,
        "the subscription-open refusal forwarded with `?` carries the same code: {payload}"
    );
    ws.expect_complete(Duration::from_secs(5)).await;

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s115_an_anonymous_json_request_is_refused_with_its_documented_body() {
    use br_core_auth::PassportHeader;

    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let service = boot_graphql_service(&db, nats.nats().await, "se_s115j", "pod-s115j").await;

    let response = reqwest::Client::new()
        .post(service.http("/graphql"))
        .json(&serde_json::json!({ "query": "{ __typename }" }))
        .send()
        .await
        .expect("the POST reaches the server");
    assert_eq!(response.status().as_u16(), 401);
    assert_eq!(
        response.text().await.unwrap_or_default(),
        "the X-Passport header is absent",
        "the anonymous JSON refusal keeps its status and its body, the body never read"
    );

    let response = reqwest::Client::new()
        .post(service.http("/graphql"))
        .header("x-passport", "not-a-valid-passport")
        .json(&serde_json::json!({ "query": "{ __typename }" }))
        .send()
        .await
        .expect("the POST reaches the server");
    assert_eq!(response.status().as_u16(), 401);
    assert!(
        response
            .text()
            .await
            .unwrap_or_default()
            .starts_with("the X-Passport header is malformed"),
        "the malformed-passport JSON refusal keeps its body"
    );

    let (status, body) = post_json(
        &service.base_url,
        Some(&passport_for(Uuid::now_v7(), Uuid::now_v7()).to_header()),
        "{ __typename }",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 200,
        "an authenticated JSON request is served: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
