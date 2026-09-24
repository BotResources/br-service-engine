use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::{StreamExt, stream};

use super::{BodyPolicy, BodyRefusal, receive};
use crate::config::{DEFAULT_BODY_READ_TIMEOUT, DEFAULT_MAX_BODY_BYTES};
use crate::graphql::multipart::{MultipartConfig, MultipartPolicy, MultipartRefusal};
use crate::graphql::{BODY_READ_TIMEOUT_CODE, BODY_TOO_LARGE_CODE};

const FILE_REQUEST: &str = "--b\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n\
    {\"query\":\"{ok}\",\"variables\":{\"f\":null}}\r\n\
    --b\r\nContent-Disposition: form-data; name=\"map\"\r\n\r\n{\"0\":[\"variables.f\"]}\r\n\
    --b\r\nContent-Disposition: form-data; name=\"0\"; filename=\"a\"\r\n\r\nx\r\n--b--\r\n";

fn headers(content_type: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
    headers
}

fn no_upload() -> BodyPolicy {
    bounded_to(DEFAULT_MAX_BODY_BYTES, DEFAULT_BODY_READ_TIMEOUT)
}

fn bounded_to(max_body_bytes: u64, read_timeout: Duration) -> BodyPolicy {
    BodyPolicy::new(
        max_body_bytes,
        read_timeout,
        MultipartPolicy::new(&MultipartConfig::default(), max_body_bytes, false),
    )
}

fn declaring(content_type: &str, length: usize) -> HeaderMap {
    let mut headers = headers(content_type);
    headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    headers
}

fn chunks_then_never(chunks: &[&'static str]) -> Body {
    let sent = stream::iter(
        chunks
            .iter()
            .map(|chunk| Ok::<_, std::io::Error>(Bytes::from_static(chunk.as_bytes())))
            .collect::<Vec<_>>(),
    );
    let never = stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("the body was read past the chunk that crossed the limit")
        },
    );
    Body::from_stream(sent.chain(never))
}

fn stalls_after(head: &'static str) -> Body {
    let head =
        stream::once(async move { Ok::<_, std::io::Error>(Bytes::from_static(head.as_bytes())) });
    Body::from_stream(head.chain(stream::pending()))
}

async fn rendered(refusal: BodyRefusal) -> (StatusCode, serde_json::Value) {
    let response = refusal.into_response();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the refusal body is readable");
    (
        status,
        serde_json::from_slice(&body).expect("the refusal body is JSON"),
    )
}

fn never_polled() -> Body {
    Body::from_stream(stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("the body was read")
        },
    ))
}

#[tokio::test]
async fn every_multipart_spelling_reaches_the_bounded_receiver() {
    for content_type in [
        "multipart/form-data; boundary=b",
        "Multipart/Form-Data; boundary=b",
        "MULTIPART/FORM-DATA; boundary=\"b\"",
    ] {
        let verdict = receive(
            &headers(content_type),
            Body::from(FILE_REQUEST),
            &no_upload(),
        )
        .await;
        assert!(
            matches!(
                verdict,
                Err(BodyRefusal::Multipart(MultipartRefusal::TooManyFiles {
                    limit: 0
                }))
            ),
            "`{content_type}` is judged by the engine's receiver, which takes no file here"
        );
    }
    let mixed = receive(
        &headers("multipart/mixed; boundary=b"),
        Body::from(FILE_REQUEST),
        &no_upload(),
    )
    .await;
    assert!(
        matches!(
            mixed,
            Err(BodyRefusal::Multipart(MultipartRefusal::Malformed(_)))
        ),
        "another multipart subtype is refused by the engine's receiver, never parsed upstream"
    );
}

#[tokio::test]
async fn a_json_body_is_parsed_as_the_extractor_parsed_it() {
    let body = Body::from(r#"{"query":"{ ok }"}"#);
    let request = receive(&headers("application/json"), body, &no_upload()).await;
    assert!(matches!(request, Ok(ref request) if request.query == "{ ok }"));

    let request = receive(
        &HeaderMap::new(),
        Body::from(r#"{"query":"{ ok }"}"#),
        &no_upload(),
    )
    .await;
    assert!(
        matches!(request, Ok(ref request) if request.query == "{ ok }"),
        "no content type defaults to JSON, as upstream"
    );

    let batch = receive(
        &headers("application/json"),
        Body::from(r#"[{"query":"{ a }"},{"query":"{ b }"}]"#),
        &no_upload(),
    )
    .await;
    assert!(
        matches!(batch, Err(BodyRefusal::Parse(_))),
        "a batch stays refused"
    );
}

#[tokio::test]
async fn an_unparseable_content_type_is_refused_before_the_body_is_read() {
    let verdict = receive(&headers("not a/mime;;"), never_polled(), &no_upload()).await;
    assert!(matches!(verdict, Err(BodyRefusal::Parse(_))));
}

#[tokio::test]
async fn a_json_body_declared_over_max_body_bytes_is_refused_unread() {
    let verdict = receive(
        &declaring("application/json", 11),
        never_polled(),
        &bounded_to(10, DEFAULT_BODY_READ_TIMEOUT),
    )
    .await;
    assert!(
        matches!(verdict, Err(BodyRefusal::TooLarge { limit: 10 })),
        "a declared length over the bound is refused before a byte is read"
    );
}

#[tokio::test]
async fn a_streamed_json_body_is_cut_on_the_chunk_that_crosses_max_body_bytes() {
    let verdict = receive(
        &headers("application/json"),
        chunks_then_never(&[r#"{"query":"#, r#""{ ok }"}"#]),
        &bounded_to(10, DEFAULT_BODY_READ_TIMEOUT),
    )
    .await;
    assert!(
        matches!(verdict, Err(BodyRefusal::TooLarge { limit: 10 })),
        "a body with no declared length is cut as it crosses the bound, never read further"
    );
}

#[tokio::test]
async fn a_json_body_exactly_at_max_body_bytes_is_served() {
    let json = r#"{"query":"{ ok }"}"#;
    let request = receive(
        &declaring("application/json", json.len()),
        Body::from(json),
        &bounded_to(json.len() as u64, DEFAULT_BODY_READ_TIMEOUT),
    )
    .await;
    assert!(
        matches!(request, Ok(ref request) if request.query == "{ ok }"),
        "the bound is inclusive: a body of exactly max_body_bytes is read"
    );
}

#[tokio::test]
async fn a_body_that_stalls_is_refused_at_the_read_timeout_whatever_its_type() {
    let timeout = Duration::from_millis(100);
    for (content_type, head) in [
        ("application/json", r#"{"query":"{ o"#),
        (
            "multipart/form-data; boundary=b",
            "--b\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n{\"query\":",
        ),
    ] {
        let started = Instant::now();
        let verdict = tokio::time::timeout(
            Duration::from_secs(5),
            receive(
                &headers(content_type),
                stalls_after(head),
                &bounded_to(1024, timeout),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("`{content_type}`: a stalled body was waited for with no end"));
        let waited = started.elapsed();
        assert!(
            matches!(verdict, Err(BodyRefusal::ReadTimeout { after }) if after == timeout),
            "`{content_type}`: a body that never finishes is abandoned at the deadline"
        );
        assert!(
            waited >= timeout,
            "`{content_type}`: refused at the deadline, not before it: {waited:?}"
        );
    }
}

#[tokio::test]
async fn the_body_refusals_are_coded_graphql_errors() {
    let (status, body) = rendered(BodyRefusal::TooLarge { limit: 10 }).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        body["errors"][0]["extensions"]["code"],
        serde_json::json!(BODY_TOO_LARGE_CODE)
    );

    let (status, body) = rendered(BodyRefusal::ReadTimeout {
        after: Duration::from_secs(30),
    })
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(
        body["errors"][0]["extensions"]["code"],
        serde_json::json!(BODY_READ_TIMEOUT_CODE)
    );
}
