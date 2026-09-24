mod multipart_support;

use std::time::{Duration, Instant};

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{GraphqlService, boot_upload_service_with};
use multipart_support::{
    BOUNDARY, JSON, chunked_request, code, map, multipart_type, operations, post_multipart,
    spool_holds_no_named_file, stalled_request, upload_form, valid_passport,
};
use service_engine::graphql::{BODY_READ_TIMEOUT_CODE, BODY_TOO_LARGE_CODE, MultipartConfig};

const MAX_BODY_BYTES: u64 = 16 * 1024;

fn json_query_padded_to(size: usize) -> String {
    let query = format!("{{ __typename }} #{}", "x".repeat(size));
    serde_json::json!({ "query": query }).to_string()
}

async fn post_json(
    service: &GraphqlService,
    passport: Option<&str>,
    body: String,
) -> (u16, serde_json::Value) {
    let mut request = reqwest::Client::new()
        .post(service.http("/graphql"))
        .header("content-type", JSON)
        .body(body);
    if let Some(passport) = passport {
        request = request.header("x-passport", passport);
    }
    let response = request.send().await.expect("the POST reaches the server");
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    (
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    )
}

#[tokio::test]
async fn s242_an_authenticated_json_body_over_max_body_bytes_is_refused_with_its_code() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let service =
        boot_upload_service_with(&db, nats.nats().await, "se_s242a", "pod-s242a", |config| {
            config.with_max_body_bytes(MAX_BODY_BYTES)
        })
        .await;
    let passport = valid_passport();

    let (status, body) = post_json(&service, Some(&passport), json_query_padded_to(8 * 1024)).await;
    assert!(
        status == 200 && body["data"]["__typename"].is_string(),
        "control: a JSON body under max_body_bytes is served: {status} {body}"
    );

    let (status, body) =
        post_json(&service, Some(&passport), json_query_padded_to(32 * 1024)).await;
    assert_eq!(
        (status, code(&body)),
        (413, &serde_json::json!(BODY_TOO_LARGE_CODE)),
        "a JSON body over EngineConfig::max_body_bytes is refused with its code: {body}"
    );

    let answer = stalled_request(
        &service,
        Some(&passport),
        JSON,
        br#"{"query":"{ __typ"#,
        32 * 1024,
        Duration::from_secs(5),
    )
    .await
    .expect("a declared length over the bound is answered without waiting for the body");
    assert!(
        answer.starts_with("HTTP/1.1 413") && answer.contains(BODY_TOO_LARGE_CODE),
        "a declared Content-Length over max_body_bytes is refused before the body is read: \
         {answer}"
    );

    let answer = chunked_request(
        &service,
        &passport,
        JSON,
        json_query_padded_to(32 * 1024).as_bytes(),
    )
    .await
    .expect("the pod answers a chunked JSON body it cuts");
    assert!(
        answer.starts_with("HTTP/1.1 413") && answer.contains(BODY_TOO_LARGE_CODE),
        "a chunked JSON body, with no length to judge up front, is cut at max_body_bytes: \
         {answer}"
    );

    let (status, body) = post_json(&service, None, json_query_padded_to(32 * 1024)).await;
    assert_eq!(
        (status, body),
        (401, serde_json::json!("the X-Passport header is absent")),
        "the passport is still judged first: an anonymous oversized body is a 401, not a 413"
    );

    let (status, body) = post_json(&service, Some(&passport), json_query_padded_to(8 * 1024)).await;
    assert_eq!(
        status, 200,
        "after the refusals the pod still serves a JSON body within its bound: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s242_a_body_that_does_not_arrive_within_the_read_timeout_is_refused_with_its_code() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let spool = tempfile::tempdir().expect("a spool directory");
    let read_timeout = Duration::from_secs(1);
    let service =
        boot_upload_service_with(&db, nats.nats().await, "se_s242b", "pod-s242b", |config| {
            config
                .with_multipart(MultipartConfig::default().with_spool_dir(spool.path()))
                .with_body_read_timeout(read_timeout)
        })
        .await;
    let passport = valid_passport();

    let inside_a_file_part = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n{ops}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"map\"\r\n\r\n{map}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"0\"; filename=\"a.bin\"\r\n\r\n\
         {partial}",
        ops = operations(&["a.bin:1:0000000000000000".to_string()]),
        map = map(1),
        partial = "a".repeat(4096),
    );
    let multipart = multipart_type();
    for (case, content_type, head_of_body) in [
        ("a JSON body", JSON, br#"{"query":"{ __typ"#.to_vec()),
        (
            "a multipart body stalled inside a file part being spooled",
            multipart.as_str(),
            inside_a_file_part.into_bytes(),
        ),
    ] {
        let started = Instant::now();
        let answer = stalled_request(
            &service,
            Some(&passport),
            content_type,
            &head_of_body,
            1024,
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("{case}: the pod still waits for a body past its deadline"));
        let waited = started.elapsed();
        assert!(
            answer.starts_with("HTTP/1.1 408") && answer.contains(BODY_READ_TIMEOUT_CODE),
            "{case}: a body that never finishes is refused with its code: {answer}"
        );
        assert!(
            waited >= read_timeout,
            "{case}: refused at the read timeout, not before it ({waited:?})"
        );
        assert!(
            spool_holds_no_named_file(spool.path()),
            "{case}: the abandoned read leaves no named file in the spool"
        );
    }

    let answer = stalled_request(
        &service,
        None,
        JSON,
        br#"{"query":"{ __typ"#,
        1024,
        Duration::from_millis(500),
    )
    .await
    .expect("an anonymous request is answered before its deadline, at once");
    assert!(
        answer.starts_with("HTTP/1.1 401"),
        "an anonymous stalled body is still refused before it is read: {answer}"
    );

    let (status, body, _) = post_multipart(
        &service,
        Some(&passport),
        upload_form(&[("a.txt", b"alpha".to_vec())]),
    )
    .await;
    assert_eq!(
        (status, &body["data"]["sampleUploadVerify"]),
        (200, &serde_json::json!(true)),
        "a body that arrives in time is served after the refusals: {body}"
    );
    let (status, body) = post_json(&service, Some(&passport), json_query_padded_to(16)).await;
    assert_eq!(status, 200, "and so is a JSON body: {body}");

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
