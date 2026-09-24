use super::*;

#[tokio::test]
async fn a_declared_length_over_the_body_limit_is_refused_before_the_body_is_polled() {
    let never_polled = stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("the body of an over-long declared request was polled")
        },
    );
    let policy = policy(MultipartConfig::default().with_max_file_bytes(512));
    match refusal_within(1024, never_polled, Some(1025), &policy).await {
        MultipartRefusal::TooLarge { limit } => assert_eq!(limit, 1024),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn a_streamed_body_crossing_the_body_limit_is_cut_mid_file() {
    let (config, _dir) = spooled(MultipartConfig::default().with_max_file_bytes(400));
    let body = body(&[
        part("operations", None, OPERATIONS.as_bytes()),
        part("map", None, br#"{"0":["variables.files.0"]}"#),
        part("0", Some("a.bin"), &[b'x'; 300]),
    ]);
    match refusal_within(400, trickle(body), None, &policy(config)).await {
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
    let file_part_never_arrives = stream::iter([Ok(Bytes::from(head))]).chain(stream::pending());
    let policy = policy(MultipartConfig::default().with_max_files(1));
    let verdict = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        refusal(file_part_never_arrives, None, &policy),
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
    let policy = MultipartPolicy::new(&MultipartConfig::default(), DEFAULT_MAX_BODY_BYTES, false);
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
