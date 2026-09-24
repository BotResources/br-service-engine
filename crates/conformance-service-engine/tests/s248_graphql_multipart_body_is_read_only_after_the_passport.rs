mod multipart_support;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{
    boot_graphql_service, boot_upload_service, boot_upload_service_with,
};
use multipart_support::{
    chunked_multipart, code, expected, map, operations, post_multipart, raw_upload_body,
    spool_holds_no_named_file, stalled_body, upload_form, valid_passport,
};
use reqwest::multipart::{Form, Part};
use service_engine::graphql::{
    MULTIPART_FILE_TOO_LARGE_CODE, MULTIPART_MALFORMED_CODE, MULTIPART_SPOOL_UNAVAILABLE_CODE,
    MULTIPART_TOO_LARGE_CODE, MULTIPART_TOO_MANY_FILES_CODE, MultipartConfig,
};

#[tokio::test]
async fn s248_an_unauthenticated_multipart_body_is_refused_before_a_byte_of_it_is_read() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let unwritable = scratch.path().join("absent");
    let service = boot_upload_service(
        &db,
        nats.nats().await,
        "se_s248a",
        "pod-s248a",
        MultipartConfig::default().with_spool_dir(&unwritable),
    )
    .await;

    let passport = valid_passport();
    let (status, body, _) = post_multipart(
        &service,
        Some(&passport),
        upload_form(&[("a.txt", b"alpha".to_vec())]),
    )
    .await;
    assert_eq!(
        (status, code(&body)),
        (500, &serde_json::json!(MULTIPART_SPOOL_UNAVAILABLE_CODE)),
        "control: an authenticated upload does reach this unwritable spool: {body}"
    );
    let detail = body.to_string();
    assert!(
        !detail.contains("absent") && !detail.contains("No such file"),
        "the spool refusal names neither the path nor the OS error: {body}"
    );

    let not_a_passport = "not-a-valid-passport";
    let decodes_to_no_passport = "eyJub3QiOiJhIHBhc3Nwb3J0In0";
    for (label, header) in [
        ("absent", None),
        ("undecodable", Some(not_a_passport)),
        ("not a passport", Some(decodes_to_no_passport)),
    ] {
        let (status, _, text) = post_multipart(
            &service,
            header,
            upload_form(&[("a.txt", b"alpha".to_vec())]),
        )
        .await;
        assert_eq!(
            status, 401,
            "a multipart request whose passport is {label} is refused as unauthenticated, before \
             the spool is reached (it would answer 500 otherwise): {text}"
        );

        let answer = stalled_body(&service, header).await.unwrap_or_else(|| {
            panic!("passport {label}: the pod did not answer a body it never received — it waited to read it")
        });
        assert!(
            answer.starts_with("HTTP/1.1 401"),
            "passport {label}: the refusal comes before the body is read: {answer}"
        );
    }
    assert_eq!(
        stalled_body(&service, Some(&passport)).await,
        None,
        "control: with a valid passport the pod reads the body, so it waits for the rest"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s248_an_authenticated_multipart_request_within_its_bounds_is_served() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let spool = tempfile::tempdir().expect("a spool directory");
    let service = boot_upload_service(
        &db,
        nats.nats().await,
        "se_s248b",
        "pod-s248b",
        MultipartConfig::default().with_spool_dir(spool.path()),
    )
    .await;
    let plain = boot_graphql_service(&db, nats.nats().await, "se_s248c", "pod-s248c").await;
    let passport = valid_passport();

    let (status, body, _) = post_multipart(
        &service,
        Some(&passport),
        upload_form(&[("a.txt", b"alpha".to_vec()), ("b.txt", b"bravo!".to_vec())]),
    )
    .await;
    assert_eq!(status, 200, "an upload within bounds is served: {body}");
    assert_eq!(
        body["data"]["sampleUploadVerify"],
        serde_json::json!(true),
        "the resolver reads each file back as it was sent: {body}"
    );
    assert!(
        spool_holds_no_named_file(spool.path()),
        "a served upload leaves no named file"
    );

    let (status, body, _) = post_multipart(
        &plain,
        Some(&passport),
        Form::new()
            .text("operations", r#"{"query":"{ __typename }"}"#)
            .text("map", "{}"),
    )
    .await;
    assert_eq!(
        status, 200,
        "multipart stays accepted on a schema without Upload: {body}"
    );
    let (status, body, _) = post_multipart(
        &plain,
        Some(&passport),
        upload_form(&[("a.txt", b"alpha".to_vec())]),
    )
    .await;
    assert_eq!(
        (status, code(&body)),
        (413, &serde_json::json!(MULTIPART_TOO_MANY_FILES_CODE)),
        "a schema that declares no Upload accepts no file, so nothing is spooled: {body}"
    );

    plain.shutdown().await;
    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s248_an_authenticated_multipart_request_over_a_bound_is_refused_with_its_code() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let spool = tempfile::tempdir().expect("a spool directory");
    let service =
        boot_upload_service_with(&db, nats.nats().await, "se_s248d", "pod-s248d", |config| {
            config.with_max_body_bytes(16 * 1024).with_multipart(
                MultipartConfig::default()
                    .with_spool_dir(spool.path())
                    .with_max_file_bytes(8 * 1024)
                    .with_max_files(2),
            )
        })
        .await;
    let passport = valid_passport();

    let cases = [
        (
            "a body over max_body_bytes",
            upload_form(&[
                ("a.bin", vec![b'a'; 8 * 1024]),
                ("b.bin", vec![b'b'; 8 * 1024]),
            ]),
            413,
            MULTIPART_TOO_LARGE_CODE,
        ),
        (
            "a file over max_file_bytes",
            upload_form(&[("a.bin", vec![b'a'; 8 * 1024 + 1])]),
            413,
            MULTIPART_FILE_TOO_LARGE_CODE,
        ),
        (
            "more uploads than max_files",
            upload_form(&[
                ("a.txt", b"a".to_vec()),
                ("b.txt", b"b".to_vec()),
                ("c.txt", b"c".to_vec()),
            ]),
            413,
            MULTIPART_TOO_MANY_FILES_CODE,
        ),
        (
            "`map` before `operations`",
            Form::new()
                .text("map", map(1))
                .text(
                    "operations",
                    operations(&expected(&[("a.txt", b"a".to_vec())])),
                )
                .part("0", Part::bytes(b"a".to_vec()).file_name("a.txt")),
            400,
            MULTIPART_MALFORMED_CODE,
        ),
    ];
    for (case, form, expected_status, expected_code) in cases {
        let (status, body, _) = post_multipart(&service, Some(&passport), form).await;
        assert_eq!(
            (status, code(&body)),
            (expected_status, &serde_json::json!(expected_code)),
            "{case}: refused with its status and code: {body}"
        );
        assert!(
            spool_holds_no_named_file(spool.path()),
            "{case}: a refusal leaves no named file in the spool"
        );
    }

    let over = [b'c'; 8 * 1024];
    let answer = chunked_multipart(&service, &passport, &raw_upload_body(&[&over, &over]))
        .await
        .expect("the pod answers a chunked body it cuts");
    assert!(
        answer.starts_with("HTTP/1.1 413") && answer.contains(MULTIPART_TOO_LARGE_CODE),
        "a chunked body, with no length to judge up front, is cut at max_body_bytes: {answer}"
    );

    let (status, body, _) = post_multipart(
        &service,
        Some(&passport),
        upload_form(&[("a.txt", b"alpha".to_vec()), ("b.txt", b"bravo".to_vec())]),
    )
    .await;
    assert_eq!(
        (status, &body["data"]["sampleUploadVerify"]),
        (200, &serde_json::json!(true)),
        "after the refusals the pod still serves an upload at its bounds: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
