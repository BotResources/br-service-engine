use super::*;

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
    let request = receive(
        CONTENT_TYPE,
        None,
        chunks(body),
        DEFAULT_MAX_BODY_BYTES,
        &policy(config),
    )
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
        DEFAULT_MAX_BODY_BYTES,
        &MultipartPolicy::new(&MultipartConfig::default(), DEFAULT_MAX_BODY_BYTES, false),
    )
    .await
    .expect("multipart stays accepted without files");
    assert_eq!(request.query, "{ok}");
    assert!(request.uploads.is_empty());
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
    let unbounded = receive(
        "multipart/form-data",
        None,
        chunks(Vec::new()),
        DEFAULT_MAX_BODY_BYTES,
        &policy,
    )
    .await;
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
