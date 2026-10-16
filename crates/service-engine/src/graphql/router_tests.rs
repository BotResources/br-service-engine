use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;

use super::Denied;
use crate::error::EngineError;
use crate::graphql::principal::AuthReject;
use crate::graphql::{INTERNAL_CODE, INTERNAL_MESSAGE, UNAUTHENTICATED_CODE};

async fn rendered(denied: Denied) -> (StatusCode, Option<String>, String) {
    let response = denied.into_response();
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the refusal body is readable");
    (
        status,
        content_type,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

#[tokio::test]
async fn every_refused_passport_is_a_401_graphql_envelope_coded_unauthenticated() {
    for reject in [
        AuthReject::Missing,
        AuthReject::Malformed("invalid base64 at offset 3".into()),
        AuthReject::Rejected("no tenant".into()),
    ] {
        let (status, content_type, body) = rendered(Denied::Unauthenticated(reject)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(content_type.as_deref(), Some("application/json"));
        let json: serde_json::Value = serde_json::from_str(&body).expect("a GraphQL-shaped body");
        assert_eq!(
            json,
            serde_json::json!({
                "errors": [{
                    "message": "unauthorized",
                    "extensions": { "code": UNAUTHENTICATED_CODE },
                }],
            })
        );
        for leak in ["X-Passport", "malformed", "rejected", "base64", "tenant"] {
            assert!(
                !body.contains(leak),
                "the client learns the code, not the rejection detail (`{leak}`): {body}"
            );
        }
    }
}

#[tokio::test]
async fn a_fact_load_failure_is_a_coded_500_that_names_no_cause() {
    let fault = EngineError::Db(sqlx::Error::Protocol(
        "permission denied for table sample_member".into(),
    ));
    let (status, _content_type, body) = rendered(Denied::FactsUnavailable(Box::new(fault))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let json: serde_json::Value = serde_json::from_str(&body).expect("a GraphQL-shaped body");
    assert_eq!(
        (
            &json["errors"][0]["message"],
            &json["errors"][0]["extensions"]["code"]
        ),
        (
            &serde_json::json!(INTERNAL_MESSAGE),
            &serde_json::json!(INTERNAL_CODE)
        )
    );
    for leak in ["permission", "sample_member", "database", "passport"] {
        assert!(
            !body.contains(leak),
            "the client learns the code, not the cause (`{leak}`): {body}"
        );
    }
}
