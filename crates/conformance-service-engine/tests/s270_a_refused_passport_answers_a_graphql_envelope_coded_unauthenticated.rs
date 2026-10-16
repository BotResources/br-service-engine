mod multipart_support;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::boot_upload_service;
use multipart_support::{post_multipart, upload_form, valid_passport};
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use service_engine::graphql::MultipartConfig;
use uuid::Uuid;

const QUERY: &str = "{ __typename }";
const DECODES_TO_NO_PASSPORT: &str = "eyJub3QiOiJhIHBhc3Nwb3J0In0";
const LEAKS: [&str; 6] = [
    "X-Passport",
    "malformed",
    "rejected",
    "tenant",
    "base64",
    "header",
];

fn rejected_by_the_principal() -> String {
    Passport::human(
        Uuid::now_v7(),
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    )
    .to_header()
}

fn assert_unauthenticated_envelope(
    label: &str,
    status: u16,
    content_type: Option<&str>,
    text: &str,
) {
    assert_eq!(status, 401, "{label}: the refusal keeps its 401: {text}");
    assert_eq!(
        content_type,
        Some("application/json"),
        "{label}: the gateway parses JSON, never plain text: {text}"
    );
    let body: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("{label}: the refusal is not JSON ({error}): {text}"));
    assert_eq!(
        body,
        serde_json::json!({
            "errors": [{
                "message": "unauthorized",
                "extensions": { "code": "UNAUTHENTICATED" },
            }],
        }),
        "{label}: the platform error contract is UNAUTHENTICATED inside the GraphQL envelope"
    );
    for leak in LEAKS {
        assert!(
            !text.contains(leak),
            "{label}: the client learns the code, not the rejection detail (`{leak}`): {text}"
        );
    }
}

async fn json_refusal(
    base_url: &str,
    passport: Option<&str>,
    accept: Option<&str>,
) -> (u16, Option<String>, String) {
    let mut request = reqwest::Client::new()
        .post(format!("{base_url}/graphql"))
        .json(&serde_json::json!({ "query": QUERY }));
    if let Some(passport) = passport {
        request = request.header("x-passport", passport);
    }
    if let Some(accept) = accept {
        request = request.header(ACCEPT, accept);
    }
    let response = request.send().await.expect("the POST reaches the server");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    (
        status,
        content_type,
        response.text().await.unwrap_or_default(),
    )
}

#[tokio::test]
async fn s270_an_absent_malformed_or_rejected_passport_is_a_json_unauthenticated_on_every_leg() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let spool = tempfile::tempdir().expect("a spool directory");
    let service = boot_upload_service(
        &db,
        nats.nats().await,
        "se_s270",
        "pod-s270",
        MultipartConfig::default().with_spool_dir(spool.path()),
    )
    .await;
    let rejected = rejected_by_the_principal();

    let refusals: [(&str, Option<&str>); 4] = [
        ("absent", None),
        ("undecodable", Some("not-a-valid-passport")),
        ("not a passport", Some(DECODES_TO_NO_PASSPORT)),
        ("rejected by the principal", Some(rejected.as_str())),
    ];
    for (label, header) in refusals {
        let (status, content_type, text) = json_refusal(&service.base_url, header, None).await;
        assert_unauthenticated_envelope(
            &format!("json, passport {label}"),
            status,
            content_type.as_deref(),
            &text,
        );

        let (status, content_type, text) =
            json_refusal(&service.base_url, header, Some("text/event-stream")).await;
        assert_unauthenticated_envelope(
            &format!("event stream, passport {label}"),
            status,
            content_type.as_deref(),
            &text,
        );

        let response = reqwest::Client::new()
            .post(service.http("/graphql"))
            .multipart(upload_form(&[("a.txt", b"alpha".to_vec())]));
        let response = match header {
            Some(header) => response.header("x-passport", header),
            None => response,
        }
        .send()
        .await
        .expect("the multipart POST reaches the server");
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let text = response.text().await.unwrap_or_default();
        assert_unauthenticated_envelope(
            &format!("multipart, passport {label}"),
            status,
            content_type.as_deref(),
            &text,
        );
    }

    let (status, body, _) = post_multipart(
        &service,
        Some(&valid_passport()),
        upload_form(&[("a.txt", b"alpha".to_vec())]),
    )
    .await;
    assert_eq!(
        status, 200,
        "control: the same multipart request with a valid passport is served: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
