use std::io::Read;

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};

use super::refusal::MultipartRefusal;
use super::{MultipartConfig, MultipartPolicy, receive};

const BOUNDARY: &str = "engine-boundary";
const CONTENT_TYPE: &str = "multipart/form-data; boundary=engine-boundary";
const OPERATIONS: &str =
    r#"{"query":"query($files:[Upload!]!){ok}","variables":{"files":[null,null]}}"#;

fn part(name: &str, filename: Option<&str>, content: &[u8]) -> Vec<u8> {
    let disposition = match filename {
        Some(filename) => format!("form-data; name=\"{name}\"; filename=\"{filename}\""),
        None => format!("form-data; name=\"{name}\""),
    };
    let mut part =
        format!("--{BOUNDARY}\r\nContent-Disposition: {disposition}\r\n\r\n").into_bytes();
    part.extend_from_slice(content);
    part.extend_from_slice(b"\r\n");
    part
}

fn body(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut body = parts.concat();
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn chunks(body: Vec<u8>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    stream::iter([Ok(Bytes::from(body))])
}

/// The body as a network delivers it: many small reads, so a bound trips mid-part.
fn trickle(body: Vec<u8>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    let pieces: Vec<_> = body
        .chunks(37)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    stream::iter(pieces)
}

fn policy(config: MultipartConfig) -> MultipartPolicy {
    MultipartPolicy::new(&config, true)
}

fn spooled(config: MultipartConfig) -> (MultipartConfig, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a scratch spool directory");
    (config.with_spool_dir(dir.path()), dir)
}

async fn refusal(
    body: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    declared: Option<u64>,
    policy: &MultipartPolicy,
) -> MultipartRefusal {
    receive(CONTENT_TYPE, declared, body, policy)
        .await
        .expect_err("the request is refused")
}

#[tokio::test]
async fn a_well_formed_request_binds_each_file_to_its_variable_paths() {
    let (config, dir) = spooled(MultipartConfig::default());
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part(
            "map",
            None,
            br#"{"0":["variables.files.0"],"1":["variables.files.1"]}"#,
        ),
        part("0", Some("a.txt"), b"alpha"),
        part("1", Some("b.txt"), b"bravo!"),
    ]);
    let request = receive(CONTENT_TYPE, None, chunks(body), &policy(config))
        .await
        .expect("a request within every bound is received");
    assert_eq!(request.uploads.len(), 2);
    let mut contents = Vec::new();
    for upload in &request.uploads {
        let mut content = String::new();
        upload
            .try_clone()
            .expect("the spooled file clones")
            .content
            .read_to_string(&mut content)
            .expect("the spooled file reads back from its start");
        contents.push((upload.filename.clone(), content));
    }
    assert_eq!(
        contents,
        [
            ("a.txt".to_string(), "alpha".to_string()),
            ("b.txt".to_string(), "bravo!".to_string())
        ]
    );
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "a spooled file is anonymous: it leaves no name in the spool directory"
    );
}

#[tokio::test]
async fn a_request_without_files_is_received_even_when_the_schema_takes_no_upload() {
    let body = body(&[
        part("operations", None, br#"{"query":"{ok}"}"#),
        part("map", None, b"{}"),
    ]);
    let request = receive(
        CONTENT_TYPE,
        None,
        chunks(body),
        &MultipartPolicy::new(&MultipartConfig::default(), false),
    )
    .await
    .expect("multipart stays accepted without files");
    assert_eq!(request.query, "{ok}");
    assert!(request.uploads.is_empty());
}

#[tokio::test]
async fn a_declared_length_over_the_body_limit_is_refused_before_the_body_is_polled() {
    let never_polled = stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("the body of an over-long declared request was polled")
        },
    );
    let policy = policy(
        MultipartConfig::default()
            .with_max_body_bytes(1024)
            .with_max_file_bytes(512),
    );
    match refusal(never_polled, Some(1025), &policy).await {
        MultipartRefusal::TooLarge { limit } => assert_eq!(limit, 1024),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn a_streamed_body_crossing_the_body_limit_is_cut_mid_file() {
    let config = MultipartConfig::default()
        .with_max_body_bytes(400)
        .with_max_file_bytes(400);
    let (config, _dir) = spooled(config);
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, br#"{"0":["variables.files.0"]}"#),
        part("0", Some("a.bin"), &[b'x'; 300]),
    ]);
    match refusal(trickle(body), None, &policy(config)).await {
        MultipartRefusal::TooLarge { limit } => assert_eq!(limit, 400),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn a_file_over_the_part_limit_is_refused() {
    let (config, dir) = spooled(MultipartConfig::default().with_max_file_bytes(64));
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, br#"{"0":["variables.files.0"]}"#),
        part("0", Some("a.bin"), &[b'x'; 65]),
    ]);
    match refusal(trickle(body), None, &policy(config)).await {
        MultipartRefusal::FileTooLarge { limit } => assert_eq!(limit, 64),
        other => panic!("expected FileTooLarge, got {other:?}"),
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn a_map_binding_too_many_uploads_is_refused_without_waiting_for_a_file_part() {
    let mut head = [
        part("operations", None, OPERATIONS.as_bytes()),
        part(
            "map",
            None,
            br#"{"0":["variables.files.0","variables.files.1"]}"#,
        ),
    ]
    .concat();
    head.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    // The file part never arrives: a verdict that needed its bytes would hang here.
    let body = stream::iter([Ok(Bytes::from(head))]).chain(stream::pending());
    let policy = policy(MultipartConfig::default().with_max_files(1));
    let verdict = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        refusal(body, None, &policy),
    )
    .await
    .expect("the upload count is judged on `map`, never on the file parts that follow it");
    match verdict {
        MultipartRefusal::TooManyFiles { limit } => assert_eq!(limit, 1),
        other => panic!("expected TooManyFiles, got {other:?}"),
    }
}

#[tokio::test]
async fn a_schema_without_upload_accepts_no_file() {
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, br#"{"0":["variables.files.0"]}"#),
        part("0", Some("a.bin"), b"payload"),
    ]);
    let policy = MultipartPolicy::new(&MultipartConfig::default(), false);
    match refusal(chunks(body), None, &policy).await {
        MultipartRefusal::TooManyFiles { limit } => assert_eq!(limit, 0),
        other => panic!("expected TooManyFiles, got {other:?}"),
    }
}

#[tokio::test]
async fn a_map_heavier_than_its_allowance_is_refused_before_it_is_parsed() {
    let paths = vec!["variables.files.0"; 200];
    let heavy = serde_json::json!({ "0": paths }).to_string();
    assert!(
        heavy.len() > 1024 * 2,
        "the map outweighs 1 KiB per allowed upload plus one"
    );
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, heavy.as_bytes()),
    ]);
    let policy = policy(MultipartConfig::default().with_max_files(1));
    match refusal(trickle(body), None, &policy).await {
        MultipartRefusal::TooManyFiles { limit } => assert_eq!(limit, 1),
        other => panic!("expected TooManyFiles, got {other:?}"),
    }
}

#[tokio::test]
async fn a_request_that_breaks_the_spec_order_or_mapping_is_malformed() {
    let (config, _dir) = spooled(MultipartConfig::default());
    let policy = policy(config);
    let operations = || part("operations", None, OPERATIONS.as_bytes());
    let map = || part("map", None, br#"{"0":["variables.files.0"]}"#);
    let cases = [
        ("map before operations", body(&[map(), operations()])),
        ("no map", body(&[operations()])),
        (
            "a file `map` does not name",
            body(&[operations(), map(), part("9", Some("x"), b"x")]),
        ),
        (
            "a mapped file that never comes",
            body(&[operations(), map()]),
        ),
        (
            "a file part without a filename",
            body(&[operations(), map(), part("0", None, b"x")]),
        ),
        (
            "an entry bound to no path",
            body(&[operations(), part("map", None, br#"{"0":[]}"#)]),
        ),
        (
            "a batch in `operations`",
            body(&[part("operations", None, b"[]"), map()]),
        ),
    ];
    for (case, body) in cases {
        match refusal(chunks(body), None, &policy).await {
            MultipartRefusal::Malformed(_) => {}
            other => panic!("{case}: expected Malformed, got {other:?}"),
        }
    }
    let unbounded = receive("multipart/form-data", None, chunks(Vec::new()), &policy).await;
    assert!(matches!(unbounded, Err(MultipartRefusal::Malformed(_))));
}

#[tokio::test]
async fn a_spool_directory_it_cannot_write_is_refused_as_unavailable() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let config = MultipartConfig::default().with_spool_dir(dir.path().join("absent"));
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, br#"{"0":["variables.files.0"]}"#),
        part("0", Some("a.bin"), b"payload"),
    ]);
    match refusal(chunks(body), None, &policy(config)).await {
        MultipartRefusal::SpoolUnavailable(_) => {}
        other => panic!("expected SpoolUnavailable, got {other:?}"),
    }
}
