use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::Denied;
use crate::error::EngineError;
use crate::graphql::principal::AuthReject;
use crate::graphql::{INTERNAL_CODE, INTERNAL_MESSAGE};

async fn rendered(denied: Denied) -> (StatusCode, String) {
    let response = denied.into_response();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the refusal body is readable");
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn a_refused_passport_keeps_its_401_and_its_body() {
    assert_eq!(
        rendered(Denied::Unauthenticated(AuthReject::Missing)).await,
        (
            StatusCode::UNAUTHORIZED,
            "the X-Passport header is absent".to_string()
        )
    );
    assert_eq!(
        rendered(Denied::Unauthenticated(AuthReject::Rejected(
            "no tenant".into()
        )))
        .await,
        (
            StatusCode::UNAUTHORIZED,
            "the passport is rejected: no tenant".to_string()
        )
    );
}

#[tokio::test]
async fn a_fact_load_failure_is_a_coded_500_that_names_no_cause() {
    let fault = EngineError::Db(sqlx::Error::Protocol(
        "permission denied for table sample_member".into(),
    ));
    let (status, body) = rendered(Denied::FactsUnavailable(Box::new(fault))).await;
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
