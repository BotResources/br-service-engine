use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use futures_util::stream;

use super::{BodyRefusal, receive};
use crate::graphql::multipart::{MultipartConfig, MultipartPolicy, MultipartRefusal};

const FILE_REQUEST: &str = "--b\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n\
    {\"query\":\"{ok}\",\"variables\":{\"f\":null}}\r\n\
    --b\r\nContent-Disposition: form-data; name=\"map\"\r\n\r\n{\"0\":[\"variables.f\"]}\r\n\
    --b\r\nContent-Disposition: form-data; name=\"0\"; filename=\"a\"\r\n\r\nx\r\n--b--\r\n";

fn headers(content_type: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
    headers
}

fn no_upload() -> MultipartPolicy {
    MultipartPolicy::new(&MultipartConfig::default(), false)
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
