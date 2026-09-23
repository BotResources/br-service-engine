use async_graphql::ParseRequestError;
use async_graphql::http::MultipartOptions;
use async_graphql_axum::rejection::GraphQLRejection;
use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::response::{IntoResponse, Response};

use crate::graphql::multipart::{self, MultipartPolicy, MultipartRefusal};

/// Why a body yields no GraphQL request: a multipart bound or shape the engine refuses, or a
/// parse error async-graphql reports, rendered exactly as its axum extractor renders it.
pub(crate) enum BodyRefusal {
    Multipart(MultipartRefusal),
    Parse(GraphQLRejection),
}

impl IntoResponse for BodyRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::Multipart(refusal) => refusal.into_response(),
            Self::Parse(rejection) => rejection.into_response(),
        }
    }
}

/// Reads the GraphQL request out of an authenticated `POST /graphql` body.
///
/// A `multipart/*` body goes through the engine's bounded receiver. Every other body is
/// parsed exactly as async-graphql-axum's `GraphQLRequest` extractor parses it — same
/// content-type dispatch, same single-request rule, same rejection — so a JSON client sees
/// no change.
pub(crate) async fn receive(
    headers: &HeaderMap,
    body: Body,
    policy: &MultipartPolicy,
) -> Result<async_graphql::Request, BodyRefusal> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if let Some(content_type) = content_type.filter(|value| is_multipart(value)) {
        let declared_length = headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        return multipart::receive(
            content_type,
            declared_length,
            body.into_data_stream(),
            policy,
        )
        .await
        .map_err(BodyRefusal::Multipart);
    }
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|error| {
            rejection(ParseRequestError::Io(std::io::Error::other(
                error.to_string(),
            )))
        })?;
    async_graphql::http::receive_body(content_type, bytes.as_ref(), MultipartOptions::default())
        .await
        .map_err(rejection)
}

/// The same test async-graphql applies before it hands a body to its own multipart parser,
/// so no multipart body can reach that unbounded path.
fn is_multipart(content_type: &str) -> bool {
    content_type
        .parse::<mime::Mime>()
        .is_ok_and(|mime| mime.type_() == mime::MULTIPART)
}

fn rejection(error: ParseRequestError) -> BodyRefusal {
    BodyRefusal::Parse(GraphQLRejection(error))
}
